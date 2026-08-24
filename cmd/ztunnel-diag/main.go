package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
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
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"ztunnel-diag/internal/report"
	"ztunnel-diag/internal/ztlog"
)

func main() {
	kubeconfig := flag.String("kubeconfig", "", "path to kubeconfig (defaults to $KUBECONFIG, then ~/.kube/config, then in-cluster config)")
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
	initContainerMode := flag.String("init-container-mode", initContainerModeNone, "mitigation init container to add to each burst pod: none | noop (bare exit 0, no logic — isolates whether adding any init container's own start overhead matters) | probe (retries a real HTTPS request to the k8s API server, via its injected env vars not DNS, until it gets a genuine HTTP response back) | mixed (alternates noop/probe by pod index within this one burst, so both experience identical congestion — prints a correlation between noop's transition overhead and probe's measured identity-wait)")
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

	// ztunnel's own logs tag a connection by its dst.service (the original
	// Service DNS name) regardless of whether the client resolved it via DNS
	// or hit the ClusterIP directly — captured before resolution so
	// buildPodEvents can tell the workload's real connection apart from a
	// mitigation init container's own probe (see ztlog.RoutingDelay).
	targetHost, _, err := net.SplitHostPort(*target)
	if err != nil {
		log.Fatalf("parsing --target %q: %v", *target, err)
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

	createdAt, podNames, podModes := createBurst(ctx, clientset, *namespace, runID, *count, *targetNode, *target, *initContainerMode)
	if !*keep {
		defer deleteBurst(ctx, clientset, *namespace, podNames)
	}

	patchTimes := collectPodIPs(ctx, ipWatch, podNames, *window)

	log.Printf("waiting %s for in-flight connections to settle before scraping ztunnel logs", *settle)
	time.Sleep(*settle)

	stopHostLoad()

	events := buildPodEvents(ctx, clientset, *namespace, *ztunnelNamespace, *ztunnelLabel, targetHost, podNames, createdAt, patchTimes, podModes)
	rep := report.Compute(events)

	if *initContainerMode == initContainerModeMixed {
		printMixedModeCorrelation(rep.Pods)
	}

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

func buildClientset(kubeconfigPath string) (*kubernetes.Clientset, error) {
	cfg, err := buildRESTConfig(kubeconfigPath)
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

func buildRESTConfig(kubeconfigPath string) (*rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		loadingRules.ExplicitPath = kubeconfigPath
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{}).ClientConfig()
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
// the node's kubelet) as close to simultaneously as possible. It also returns
// each pod's actual init container mode, which only varies per-pod under
// initContainerModeMixed — see podModeForIndex.
func createBurst(ctx context.Context, cs *kubernetes.Clientset, ns, runID string, count int, targetNode, target, initContainerMode string) (map[string]time.Time, []string, map[string]string) {
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
			pod := podSpec(name, runID, sa, targetNode, target, podModeForIndex(initContainerMode, i))
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
	modeByName := make(map[string]string, count)
	for i, name := range names {
		if !createdAt[i].IsZero() {
			byName[name] = createdAt[i]
		}
		modeByName[name] = podModeForIndex(initContainerMode, i)
	}
	return byName, names, modeByName
}

const exitMarkerPrefix = "ztunnel-diag-exit="

// Init container modes for podSpec, controlled by --init-container-mode.
const (
	initContainerModeNone  = "none"
	initContainerModeNoop  = "noop"
	initContainerModeProbe = "probe"
	// initContainerModeMixed alternates noop/probe by pod index within one
	// burst, so both experience identical moment-by-moment congestion.
	// See README "Does noop's delay scale with the problem?".
	initContainerModeMixed = "mixed"
)

// podModeForIndex resolves the per-pod init container mode for pod i of a
// burst.
func podModeForIndex(mode string, i int) string {
	if mode != initContainerModeMixed {
		return mode
	}
	if i%2 == 0 {
		return initContainerModeNoop
	}
	return initContainerModeProbe
}

// initContainerRetries, initContainerProbeTimeout and
// initContainerRetryInterval bound the mitigation init container's probe
// loop (see podSpec): up to ~80s worst case. initContainerProbeTimeout (a
// wget -T value, in seconds) is deliberately longer than ztunnel's 5s hold so
// a single attempt can observe either a genuine forward (a real HTTP
// response) or ztunnel's own wait-then-drop within that one attempt, rather
// than always bailing out before either has resolved.
const (
	initContainerRetries       = 20
	initContainerProbeTimeout  = "3"
	initContainerRetryInterval = "1"
)

// initOutcomeMarkerPrefix tags the init container's own log with whether its
// probe loop found ztunnel ready or exhausted its retry budget — the
// workload's own exit code (see connectionFailed) says whether the real
// connection succeeded, but not whether the mitigation got there early or
// rode the retry budget all the way down.
const initOutcomeMarkerPrefix = "ztunnel-diag-init-outcome="

// podSpec builds the burst workload pod, pinned to targetNode (by hostname)
// so the whole burst saturates one specific node.
func podSpec(name, runID, serviceAccount, targetNode, target, initContainerMode string) *corev1.Pod {
	spec := corev1.PodSpec{
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
	}
	switch initContainerMode {
	case initContainerModeProbe:
		spec.InitContainers = []corev1.Container{{
			Name:  "wait-for-ztunnel-identity",
			Image: "busybox:1.36",
			Command: []string{"sh", "-c", fmt.Sprintf(
				`start=$(date +%%s); i=0; outcome=exhausted; `+
					`while [ $i -lt %d ]; do i=$((i+1)); `+
					`wget --no-check-certificate -T %s -O /dev/null "https://$KUBERNETES_SERVICE_HOST:$KUBERNETES_SERVICE_PORT/" 2>&1 | grep -q "HTTP/" `+
					`&& { outcome=ready; break; }; sleep %s; done; `+
					`end=$(date +%%s); echo "%s$outcome attempts=$i elapsed=$((end-start))s"; exit 0`,
				initContainerRetries, initContainerProbeTimeout, initContainerRetryInterval, initOutcomeMarkerPrefix,
			)},
		}}
	case initContainerModeNoop:
		spec.InitContainers = []corev1.Container{{
			Name:    "noop",
			Image:   "busybox:1.36",
			Command: []string{"sh", "-c", "exit 0"},
		}}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{"app": "ztunnel-diag", "run": runID},
		},
		Spec: spec,
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

// initContainerOutcome reads back the init container's own outcome marker.
func initContainerOutcome(initLog string) (ready, known bool, attempts int, elapsed time.Duration) {
	idx := strings.Index(initLog, initOutcomeMarkerPrefix)
	if idx == -1 {
		return false, false, 0, 0
	}
	rest := initLog[idx+len(initOutcomeMarkerPrefix):]
	if nl := strings.IndexByte(rest, '\n'); nl != -1 {
		rest = rest[:nl]
	}
	fields := strings.Fields(rest)
	if len(fields) != 3 {
		return false, false, 0, 0
	}
	outcome := fields[0]
	if outcome != "ready" && outcome != "exhausted" {
		return false, false, 0, 0
	}
	attemptsStr, ok := strings.CutPrefix(fields[1], "attempts=")
	if !ok {
		return false, false, 0, 0
	}
	attempts, err := strconv.Atoi(attemptsStr)
	if err != nil {
		return false, false, 0, 0
	}
	elapsedStr, ok := strings.CutPrefix(fields[2], "elapsed=")
	if !ok {
		return false, false, 0, 0
	}
	elapsedSecs, err := strconv.Atoi(strings.TrimSuffix(elapsedStr, "s"))
	if err != nil {
		return false, false, 0, 0
	}
	return outcome == "ready", true, attempts, time.Duration(elapsedSecs) * time.Second
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

// buildPodEvents turns raw timestamps into report.PodEvents.
func buildPodEvents(ctx context.Context, cs *kubernetes.Clientset, ns, ztunnelNS, ztunnelLabel, targetHost string, podNames []string, createdAt, patchTimes map[string]time.Time, podModes map[string]string) []report.PodEvent {
	ztunnelLogs := fetchZtunnelLogs(ctx, cs, ztunnelNS, ztunnelLabel)
	workloadLogs := fetchContainerLogs(ctx, cs, ns, "workload", podNames)
	var probeNames []string
	for _, name := range podNames {
		if podModes[name] == initContainerModeProbe {
			probeNames = append(probeNames, name)
		}
	}
	var initLogs map[string]string
	if len(probeNames) > 0 {
		initLogs = fetchContainerLogs(ctx, cs, ns, "wait-for-ztunnel-identity", probeNames)
	}
	firstContainerStart := fetchFirstContainerStartTimes(ctx, cs, ns, podNames)

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
		routingDelay, routingDelayKnown := ztlog.RoutingDelay(ztunnelLogs, name, ns, targetHost)
		connFailed, connFailedKnown := connectionFailed(workloadLogs[name])
		initReady, initKnown, initAttempts, initElapsed := initContainerOutcome(initLogs[name])

		var upperBound, lowerBound time.Duration
		var upperBoundKnown, lowerBoundKnown bool
		if anchor, ok := firstContainerStart[name]; ok {
			if identityReadyAt, ok := ztlog.IdentityReadyAt(ztunnelLogs, name, ns); ok {
				upperBound, upperBoundKnown = identityReadyAt.Sub(anchor), true
			}
			if lastFailureAt, ok := ztlog.LastPreSuccessFailureAt(ztunnelLogs, name, ns); ok {
				lowerBound, lowerBoundKnown = lastFailureAt.Sub(anchor), true
			}
		}

		events = append(events, report.PodEvent{
			Name:                              name,
			InitContainerMode:                 podModes[name],
			IPAssignedAt:                      created,
			IPPatchedAtAPI:                    patchedAt,
			ZtunnelTimeout:                    timedOut,
			RoutingDelay:                      routingDelay,
			RoutingDelayKnown:                 routingDelayKnown,
			ConnectionFailed:                  connFailed,
			ConnectionFailedKnown:             connFailedKnown,
			InitContainerOutcomeKnown:         initKnown,
			InitContainerReady:                initReady,
			InitContainerAttempts:             initAttempts,
			InitContainerElapsed:              initElapsed,
			CounterfactualWaitUpperBound:      upperBound,
			CounterfactualWaitUpperBoundKnown: upperBoundKnown,
			CounterfactualWaitLowerBound:      lowerBound,
			CounterfactualWaitLowerBoundKnown: lowerBoundKnown,
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

// fetchContainerLogs reads back one named container's log from each of
// podNames, concurrently.
func fetchContainerLogs(ctx context.Context, cs *kubernetes.Clientset, ns, container string, podNames []string) map[string]string {
	logs := make(map[string]string, len(podNames))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, name := range podNames {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			body, err := readPodLog(ctx, cs, ns, name, &corev1.PodLogOptions{Container: container})
			if err != nil {
				log.Printf("stream logs for %s/%s: %v", name, container, err)
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

// fetchFirstContainerStartTimes returns, for each pod, when its first
// container (init, if any, else "workload") actually started running.
func fetchFirstContainerStartTimes(ctx context.Context, cs *kubernetes.Clientset, ns string, podNames []string) map[string]time.Time {
	pods, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("list pods for container start times: %v", err)
		return nil
	}
	want := make(map[string]bool, len(podNames))
	for _, n := range podNames {
		want[n] = true
	}
	result := make(map[string]time.Time, len(podNames))
	for i := range pods.Items {
		p := &pods.Items[i]
		if !want[p.Name] {
			continue
		}
		if t, ok := firstContainerStartTime(p); ok {
			result[p.Name] = t
		}
	}
	return result
}

// firstContainerStartTime returns when kubelet actually started this pod's
// first container: the init container if one is present, else "workload".
func firstContainerStartTime(pod *corev1.Pod) (time.Time, bool) {
	if len(pod.Status.InitContainerStatuses) > 0 {
		return containerStartTime(pod.Status.InitContainerStatuses[0])
	}
	for _, c := range pod.Status.ContainerStatuses {
		if c.Name == "workload" {
			return containerStartTime(c)
		}
	}
	return time.Time{}, false
}

func containerStartTime(c corev1.ContainerStatus) (time.Time, bool) {
	switch {
	case c.State.Running != nil:
		return c.State.Running.StartedAt.Time, true
	case c.State.Terminated != nil:
		return c.State.Terminated.StartedAt.Time, true
	default:
		return time.Time{}, false
	}
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
	if rep.InitContainerKnownCount > 0 {
		fmt.Printf("  init container outcome: %d/%d ready, %d/%d exhausted their retry budget (mean %s, max %s to ready/exhausted)\n",
			rep.InitContainerReadyCount, rep.InitContainerKnownCount, rep.InitContainerExhaustedCount, rep.InitContainerKnownCount,
			rep.MeanInitContainerElapsed, rep.MaxInitContainerElapsed)
	}
	if rep.MitigationComparableCount > 0 {
		fmt.Printf("  per-pod causal verdict (of %d successful pods with counterfactual data): %d definitely unnecessary, %d definitely necessary, %d ambiguous\n",
			rep.MitigationComparableCount, rep.MitigationDefinitelyUnnecessary, rep.MitigationDefinitelyNecessary, rep.MitigationAmbiguous)
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
		initStatus := ""
		if p.InitContainerOutcomeKnown {
			outcome := "ready"
			if !p.InitContainerReady {
				outcome = "EXHAUSTED"
			}
			initStatus = fmt.Sprintf(" init=%s(%d,%s)", outcome, p.InitContainerAttempts, p.InitContainerElapsed)
		}
		verdict := ""
		if p.ConnectionFailedKnown && !p.ConnectionFailed && p.CounterfactualWaitUpperBoundKnown {
			switch {
			case p.CounterfactualWaitUpperBound <= 5*time.Second:
				verdict = fmt.Sprintf(" [unnecessary, upper=%s]", p.CounterfactualWaitUpperBound)
			case p.CounterfactualWaitLowerBoundKnown && p.CounterfactualWaitLowerBound > 5*time.Second:
				verdict = fmt.Sprintf(" [NECESSARY, lower=%s]", p.CounterfactualWaitLowerBound)
			default:
				verdict = fmt.Sprintf(" [ambiguous, upper=%s]", p.CounterfactualWaitUpperBound)
			}
		}
		fmt.Printf("  %-40s patch=%-10s %-24s %s%s%s%s\n", p.Name, p.PatchLatency, routing, conn, initStatus, verdict, flag)
	}
}

// printMixedModeCorrelation reports whether noop's container-transition
// overhead (CounterfactualWaitUpperBound, clean under noop since the
// workload's own connection resolves in microseconds once attempted — see
// README) and probe's measured identity-wait (InitContainerElapsed, its own
// retry loop's timing) co-vary across a single mixed-mode burst, where both
// experience identical moment-by-moment congestion. Pods are bucketed by
// creation order (a proxy for elapsed time into the burst) rather than
// compared pod-by-pod, since noop and probe pods are never the same pod.
//
// If the two curves move together, that's evidence noop's delay is coupled
// to the same bottleneck causing the race, not a fixed amount that happens
// to be large enough on this specific cluster. If probe's wait varies while
// noop's overhead stays flat, noop has headroom here, not scaling — the
// result that should block recommending it as a general mitigation.
func printMixedModeCorrelation(pods []report.PodResult) {
	type sample struct {
		idx   int
		value time.Duration
	}
	var noop, probe []sample
	for i, p := range pods {
		switch p.InitContainerMode {
		case initContainerModeNoop:
			if p.CounterfactualWaitUpperBoundKnown {
				noop = append(noop, sample{i, p.CounterfactualWaitUpperBound})
			}
		case initContainerModeProbe:
			if p.InitContainerOutcomeKnown {
				probe = append(probe, sample{i, p.InitContainerElapsed})
			}
		}
	}

	fmt.Println()
	fmt.Println("mixed-mode correlation (does noop's transition overhead track probe's measured identity-wait?)")
	fmt.Printf("  noop samples: %d   probe samples: %d\n", len(noop), len(probe))
	if len(noop) == 0 || len(probe) == 0 {
		fmt.Println("  not enough data on one side to compare")
		return
	}

	const buckets = 5
	bucketMeans := func(samples []sample) []float64 {
		means := make([]float64, buckets)
		counts := make([]int, buckets)
		for _, s := range samples {
			b := s.idx * buckets / len(pods)
			if b >= buckets {
				b = buckets - 1
			}
			means[b] += s.value.Seconds()
			counts[b]++
		}
		for b := range means {
			if counts[b] > 0 {
				means[b] /= float64(counts[b])
			}
		}
		return means
	}
	noopMeans := bucketMeans(noop)
	probeMeans := bucketMeans(probe)

	fmt.Printf("  %-6s", "noop")
	for _, m := range noopMeans {
		fmt.Printf(" %6.1fs", m)
	}
	fmt.Println("   (mean transition overhead, earliest-created pods left to latest)")
	fmt.Printf("  %-6s", "probe")
	for _, m := range probeMeans {
		fmt.Printf(" %6.1fs", m)
	}
	fmt.Println("   (mean measured identity-wait, same buckets)")
	fmt.Printf("  bucket-to-bucket correlation (Pearson r, -1..1): %.2f\n", pearson(noopMeans, probeMeans))
}

func pearson(a, b []float64) float64 {
	n := len(a)
	if n == 0 || n != len(b) {
		return 0
	}
	var meanA, meanB float64
	for i := range a {
		meanA += a[i]
		meanB += b[i]
	}
	meanA /= float64(n)
	meanB /= float64(n)
	var num, denomA, denomB float64
	for i := range a {
		da := a[i] - meanA
		db := b[i] - meanB
		num += da * db
		denomA += da * da
		denomB += db * db
	}
	if denomA == 0 || denomB == 0 {
		return 0
	}
	return num / math.Sqrt(denomA*denomB)
}
