package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"ztunnel-diag/internal/report"
	"ztunnel-diag/internal/ztlog"
)

func main() {
	kubeconfig := flag.String("kubeconfig", os.Getenv("KUBECONFIG"), "path to kubeconfig (defaults to $KUBECONFIG, then in-cluster config)")
	namespace := flag.String("namespace", "ztunnel-diag", "namespace to burst pods into (created if missing)")
	count := flag.Int("count", 100, "number of pods to create simultaneously (stay under the target node's pod capacity minus already-running system pods, e.g. minikube's default kubelet max-pods=110, or excess pods sit permanently Pending and pollute the report as if they were slow to patch)")
	targetNode := flag.String("target-node", "ztunnel-diag-m02", "node the burst pods are pinned to (by hostname) — the node being saturated")
	window := flag.Duration("window", 60*time.Second, "how long to wait for each pod's status.podIP to appear before giving up on it")
	settle := flag.Duration("settle", 30*time.Second, "extra time to wait after the observation window before scraping ztunnel's logs — in a large burst, later pods may still be scheduling or mid-connection (with ztunnel's own 5s hold still in flight) when the window closes")
	target := flag.String("target", "", "host:port every burst pod makes a real HTTP request against (default: echo-target.<namespace>.svc.cluster.local:8080 — see hack/deploy-echo-target.sh, pinned to the *other* node so it stays warm). If host names a Service in --namespace, it's resolved to that Service's ClusterIP before the burst starts so the workload's request skips DNS — going through DNS first lets ztunnel's own DNS-proxy identity wait get silently retried by the client resolver, masking the real outbound-connect-time hold this tool means to measure")
	ztunnelNamespace := flag.String("ztunnel-namespace", "istio-system", "namespace ztunnel daemonset pods run in")
	ztunnelLabel := flag.String("ztunnel-label", "app=ztunnel", "label selector for ztunnel pods")
	keep := flag.Bool("keep", false, "keep the burst pods around after the run instead of deleting them")
	asJSON := flag.Bool("json", false, "print the raw report as JSON instead of a human summary")
	hostLoadWorkers := flag.Int("host-load-workers", runtime.NumCPU(), "goroutines busy-looping on the host's real CPU for the duration of the burst+window+settle, to compete with minikube's Docker containers (istiod/ztunnel) for actual host scheduling time — an in-cluster CPU-hog pod only competes within the cluster's own CPU accounting and didn't move the needle. 0 disables this")
	flag.Parse()

	if *target == "" {
		*target = fmt.Sprintf("echo-target.%s.svc.cluster.local:8080", *namespace)
	}

	ctx := context.Background()

	clientset, err := buildClientset(*kubeconfig)
	if err != nil {
		log.Fatalf("building kube client: %v", err)
	}

	runID := fmt.Sprintf("%d", time.Now().UnixNano())
	if err := ensureNamespace(ctx, clientset, *namespace); err != nil {
		log.Fatalf("ensuring namespace %s: %v", *namespace, err)
	}

	resolvedTarget := resolveTargetToClusterIP(ctx, clientset, *namespace, *target)
	if resolvedTarget != *target {
		log.Printf("resolved target %s to ClusterIP %s, bypassing DNS", *target, resolvedTarget)
	}
	*target = resolvedTarget

	ipWatch, err := startPodIPWatch(ctx, clientset, *namespace, runID)
	if err != nil {
		log.Fatalf("starting pod IP watch: %v", err)
	}

	stopHostLoad := startHostLoad(*hostLoadWorkers)

	createdAt, podNames := createBurst(ctx, clientset, *namespace, runID, *count, *targetNode, *target)
	if !*keep {
		defer deleteBurst(ctx, clientset, *namespace, podNames)
	}

	patchTimes := collectPodIPs(ctx, ipWatch, podNames, *window)

	log.Printf("waiting %s for in-flight connections to settle before scraping ztunnel logs", *settle)
	time.Sleep(*settle)

	stopHostLoad()

	events := buildPodEvents(ctx, clientset, *namespace, *ztunnelNamespace, *ztunnelLabel, podNames, createdAt, patchTimes)
	rep := report.Compute(events)

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			log.Fatalf("encoding report: %v", err)
		}
		return
	}
	printSummary(rep)
}

// startHostLoad spins n goroutines doing pure CPU-bound busy work until the
// returned stop func is called. minikube's nodes are just Docker containers
// sharing the host machine's real CPU, so keeping host cores busy competes
// with istiod/ztunnel's own container processes for actual OS scheduling
// time — a more direct lever on the race than an in-cluster CPU-hog pod,
// which only competes within the cluster's own view of CPU and didn't move
// the needle (see README).
func startHostLoad(n int) (stop func()) {
	var stopped atomic.Bool
	for i := 0; i < n; i++ {
		go func() {
			for !stopped.Load() {
			}
		}()
	}
	return func() { stopped.Store(true) }
}

func buildClientset(kubeconfig string) (*kubernetes.Clientset, error) {
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}
	// client-go's default QPS/Burst (5/10) throttles a burst of concurrent
	// pod+ServiceAccount creates client-side, which would leak into the
	// patch-latency measurement as an artifact of this client rather than
	// the cluster. Raised well above any burst size this tool is meant for.
	cfg.QPS = 200
	cfg.Burst = 400
	return kubernetes.NewForConfig(cfg)
}

// resolveTargetToClusterIP rewrites a "host:port" target given by k8s Service
// DNS name into "clusterIP:port", so the workload's request skips DNS
// entirely.
func resolveTargetToClusterIP(ctx context.Context, cs kubernetes.Interface, ns, target string) string {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return target
	}
	if net.ParseIP(host) != nil {
		return target
	}
	svcName, _, _ := strings.Cut(host, ".")
	svc, err := cs.CoreV1().Services(ns).Get(ctx, svcName, metav1.GetOptions{})
	if err != nil {
		log.Printf("resolve target %s to ClusterIP: service %s/%s: %v (leaving target as-is, DNS resolution will mask ztunnel's identity wait)", target, ns, svcName, err)
		return target
	}
	if svc.Spec.ClusterIP == "" || svc.Spec.ClusterIP == corev1.ClusterIPNone {
		return target
	}
	return net.JoinHostPort(svc.Spec.ClusterIP, port)
}

func ensureNamespace(ctx context.Context, cs *kubernetes.Clientset, ns string) error {
	_, err := cs.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	_, err = cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}, metav1.CreateOptions{})
	return err
}

// createBurst creates count pods concurrently so they hit the API server (and
// the node's kubelet) as close to simultaneously as possible.
func createBurst(ctx context.Context, cs *kubernetes.Clientset, ns, runID string, count int, targetNode, target string) (map[string]time.Time, []string) {
	names := make([]string, count)
	createdAt := make([]time.Time, count)
	var mu sync.Mutex
	done := make(chan struct{}, count)

	for i := 0; i < count; i++ {
		name := fmt.Sprintf("ztunnel-diag-%s-%d", runID, i)
		names[i] = name
		go func(i int, name string) {
			defer func() { done <- struct{}{} }()
			sa := name // one ServiceAccount per pod, named after it
			if _, err := cs.CoreV1().ServiceAccounts(ns).Create(ctx, &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Name: sa, Labels: map[string]string{"app": "ztunnel-diag", "run": runID}},
			}, metav1.CreateOptions{}); err != nil {
				log.Printf("create serviceaccount %s: %v", sa, err)
				return
			}
			pod := podSpec(name, runID, sa, targetNode, target)
			t := time.Now()
			if _, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
				log.Printf("create pod %s: %v", name, err)
				return
			}
			mu.Lock()
			createdAt[i] = t
			mu.Unlock()
		}(i, name)
	}
	for i := 0; i < count; i++ {
		<-done
	}

	byName := make(map[string]time.Time, count)
	for i, name := range names {
		if !createdAt[i].IsZero() {
			byName[name] = createdAt[i]
		}
	}
	return byName, names
}

const exitMarkerPrefix = "ztunnel-diag-exit="

// podSpec builds the burst workload pod, pinned to targetNode (by hostname)
// so the whole burst saturates one specific node.
func podSpec(name, runID, serviceAccount, targetNode, target string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{"app": "ztunnel-diag", "run": runID},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: serviceAccount,
			NodeSelector:       map[string]string{"kubernetes.io/hostname": targetNode},
			RestartPolicy:      corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:  "workload",
				Image: "busybox:1.36",
				Command: []string{"sh", "-c", fmt.Sprintf(
					"wget -T 20 -O /dev/null http://%s/ 2>&1; echo %s$?; sleep 3600",
					target, exitMarkerPrefix,
				)},
			}},
		},
	}
}

func connectionFailed(workloadLog string) (failed, known bool) {
	idx := strings.LastIndex(workloadLog, exitMarkerPrefix)
	if idx == -1 {
		return false, false
	}
	rest := workloadLog[idx+len(exitMarkerPrefix):]
	end := strings.IndexByte(rest, '\n')
	if end != -1 {
		rest = rest[:end]
	}
	rest = strings.TrimSpace(rest)
	code, err := strconv.Atoi(rest)
	if err != nil {
		return false, false
	}
	return code != 0, true
}

func startPodIPWatch(ctx context.Context, cs *kubernetes.Clientset, ns, runID string) (watch.Interface, error) {
	return cs.CoreV1().Pods(ns).Watch(ctx, metav1.ListOptions{LabelSelector: "run=" + runID})
}

// collectPodIPs drains w for up to window, recording the first time each
// pod's status.podIP appears.
func collectPodIPs(ctx context.Context, w watch.Interface, podNames []string, window time.Duration) map[string]time.Time {
	patchTimes := make(map[string]time.Time, len(podNames))
	pending := make(map[string]bool, len(podNames))
	for _, n := range podNames {
		pending[n] = true
	}

	watchCtx, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	defer w.Stop()

	for len(pending) > 0 {
		select {
		case <-watchCtx.Done():
			return patchTimes
		case event, ok := <-w.ResultChan():
			if !ok {
				log.Printf("pod IP watch closed early: %d/%d pods still unresolved", len(pending), len(pending)+len(patchTimes))
				return patchTimes
			}
			if event.Type == watch.Error {
				log.Printf("pod IP watch error event: %#v", event.Object)
				continue
			}
			pod, ok := event.Object.(*corev1.Pod)
			if !ok || event.Type == watch.Deleted {
				continue
			}
			if pod.Status.PodIP == "" {
				continue
			}
			if _, seen := patchTimes[pod.Name]; seen {
				continue
			}
			patchTimes[pod.Name] = time.Now()
			delete(pending, pod.Name)
		}
	}
	return patchTimes
}

// buildPodEvents turns raw timestamps into report.PodEvents. Note IPAssignedAt
// here is the client-observed pod-creation time, not a node-local sandbox
// timestamp — see README.md "What this measures" for why that's the honest
// proxy available without shelling into the node.
func buildPodEvents(ctx context.Context, cs *kubernetes.Clientset, ns, ztunnelNS, ztunnelLabel string, podNames []string, createdAt, patchTimes map[string]time.Time) []report.PodEvent {
	ztunnelLogs := fetchZtunnelLogs(ctx, cs, ztunnelNS, ztunnelLabel)
	workloadLogs := fetchWorkloadLogs(ctx, cs, ns, podNames)

	events := make([]report.PodEvent, 0, len(podNames))
	for _, name := range podNames {
		created, gotCreate := createdAt[name]
		if !gotCreate {
			continue // create call itself failed; nothing meaningful to report
		}
		patchedAt, gotPatch := patchTimes[name]
		if !gotPatch {
			// Never observed an IP within the window: treat as maximally
			// delayed rather than silently dropping it from the report.
			patchedAt = time.Now()
		}
		timedOut := false
		for _, line := range ztunnelLogs {
			if ztlog.MatchesTimeoutForPod(line, name) {
				timedOut = true
				break
			}
		}
		routingDelay, routingDelayKnown := ztlog.RoutingDelay(ztunnelLogs, name, ns)
		connFailed, connFailedKnown := connectionFailed(workloadLogs[name])
		events = append(events, report.PodEvent{
			Name:                  name,
			IPAssignedAt:          created,
			IPPatchedAtAPI:        patchedAt,
			ZtunnelTimeout:        timedOut,
			RoutingDelay:          routingDelay,
			RoutingDelayKnown:     routingDelayKnown,
			ConnectionFailed:      connFailed,
			ConnectionFailedKnown: connFailedKnown,
		})
	}
	return events
}

func fetchZtunnelLogs(ctx context.Context, cs *kubernetes.Clientset, ns, label string) []string {
	pods, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: label})
	if err != nil {
		log.Printf("list ztunnel pods: %v", err)
		return nil
	}
	var lines []string
	for _, p := range pods.Items {
		body, err := readPodLog(ctx, cs, ns, p.Name, &corev1.PodLogOptions{SinceSeconds: int64Ptr(300)})
		if err != nil {
			log.Printf("stream logs for %s: %v", p.Name, err)
			continue
		}
		lines = append(lines, splitLines(body)...)
	}
	return lines
}

func fetchWorkloadLogs(ctx context.Context, cs *kubernetes.Clientset, ns string, podNames []string) map[string]string {
	logs := make(map[string]string, len(podNames))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, name := range podNames {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			body, err := readPodLog(ctx, cs, ns, name, &corev1.PodLogOptions{})
			if err != nil {
				log.Printf("stream logs for %s: %v", name, err)
				return
			}
			mu.Lock()
			logs[name] = body
			mu.Unlock()
		}(name)
	}
	wg.Wait()
	return logs
}

func readPodLog(ctx context.Context, cs *kubernetes.Clientset, ns, podName string, opts *corev1.PodLogOptions) (string, error) {
	req := cs.CoreV1().Pods(ns).GetLogs(podName, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", err
	}
	defer stream.Close()

	buf := make([]byte, 0, 4096)
	chunk := make([]byte, 32*1024)
	for {
		n, err := stream.Read(chunk)
		if n > 0 {
			buf = append(buf, chunk[:n]...)
		}
		if err != nil {
			break
		}
	}
	return string(buf), nil
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func int64Ptr(v int64) *int64 { return &v }

func deleteBurst(ctx context.Context, cs *kubernetes.Clientset, ns string, podNames []string) {
	grace := int64(0)
	for _, name := range podNames {
		if err := cs.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &grace}); err != nil {
			log.Printf("delete pod %s: %v", name, err)
		}
		// one ServiceAccount was created per pod, named after it — see createBurst.
		if err := cs.CoreV1().ServiceAccounts(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
			log.Printf("delete serviceaccount %s: %v", name, err)
		}
	}
}

func printSummary(rep report.Report) {
	fmt.Println("ztunnel-diag report")
	fmt.Printf("  pods observed:        %d\n", rep.Count)
	fmt.Printf("  ztunnel timeouts:     %d\n", rep.TimeoutCount)
	fmt.Printf("  mean patch latency:   %s\n", rep.MeanPatchLatency)
	fmt.Printf("  p95 patch latency:    %s\n", rep.P95PatchLatency)
	fmt.Printf("  max patch latency:    %s\n", rep.MaxPatchLatency)
	if rep.RoutingDelayCount > 0 {
		fmt.Printf("  routing delay (ztunnel's actual wait-for-workload-to-connection-opened time, %d/%d pods, requires RUST_LOG=debug):\n", rep.RoutingDelayCount, rep.Count)
		fmt.Printf("    mean: %s   max: %s\n", rep.MeanRoutingDelay, rep.MaxRoutingDelay)
	} else {
		fmt.Printf("  routing delay:        unknown for all pods (set RUST_LOG=debug on ztunnel to capture it)\n")
	}
	if rep.ConnectionFailedKnownCount > 0 {
		fmt.Printf("  connection failed (workload's own wget exit status, ground truth): %d/%d\n", rep.ConnectionFailedCount, rep.ConnectionFailedKnownCount)
	} else {
		fmt.Printf("  connection failed:    unknown for all pods (no wget exit marker found in any workload log)\n")
	}
	if rep.TimeoutVsFailureComparableCount > 0 {
		matches := rep.FailedAndTimedOut + rep.OKNotTimedOut
		fmt.Printf("  ztunnel timeout matches connection failure: %d/%d pods (failed+timeout=%d, failed+no-timeout=%d, ok+timeout=%d, ok+no-timeout=%d)\n",
			matches, rep.TimeoutVsFailureComparableCount, rep.FailedAndTimedOut, rep.FailedNotTimedOut, rep.OKButTimedOut, rep.OKNotTimedOut)
	}
	fmt.Println()
	for _, p := range rep.Pods {
		flag := ""
		if p.ZtunnelTimeout {
			flag = "  <-- ztunnel timeout"
		}
		routing := "routing delay unknown"
		if p.RoutingDelayKnown {
			routing = fmt.Sprintf("routing delay %s", p.RoutingDelay)
		}
		conn := "connection: unknown"
		if p.ConnectionFailedKnown {
			conn = "connection: ok"
			if p.ConnectionFailed {
				conn = "connection: FAILED"
			}
		}
		fmt.Printf("  %-40s patch=%-10s %-24s %s%s\n", p.Name, p.PatchLatency, routing, conn, flag)
	}
}
