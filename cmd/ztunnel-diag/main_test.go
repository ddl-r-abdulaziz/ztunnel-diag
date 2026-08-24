package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func writeKubeconfig(t *testing.T, path, server string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	content := `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: ` + server + `
  name: test-cluster
contexts:
- context:
    cluster: test-cluster
    user: test-user
  name: test-context
current-context: test-context
users:
- name: test-user
  user: {}
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestBuildRESTConfigExplicitPathWinsOverKUBECONFIGEnvVar(t *testing.T) {
	// clientcmd.BuildConfigFromFlags("", "") does NOT fall back to
	// ~/.kube/config on its own — only NewDefaultClientConfigLoadingRules
	// does (its ~/.kube/config path is a client-go package-level var fixed at
	// init time, so it can't be exercised by overriding $HOME within a test
	// binary; the explicit-path and env-var precedence below is what this
	// tool's own code controls).
	envPath := filepath.Join(t.TempDir(), "env-kubeconfig")
	writeKubeconfig(t, envPath, "https://from-env-var.example:6443")
	t.Setenv("KUBECONFIG", envPath)

	explicit := filepath.Join(t.TempDir(), "explicit-kubeconfig")
	writeKubeconfig(t, explicit, "https://explicit.example:6443")

	cfg, err := buildRESTConfig(explicit)
	if err != nil {
		t.Fatalf("buildRESTConfig(%q) error: %v", explicit, err)
	}
	if want := "https://explicit.example:6443"; cfg.Host != want {
		t.Errorf("cfg.Host = %q, want %q", cfg.Host, want)
	}
}

func TestBuildRESTConfigUsesKUBECONFIGEnvVarWhenNoExplicitPathGiven(t *testing.T) {
	envPath := filepath.Join(t.TempDir(), "env-kubeconfig")
	writeKubeconfig(t, envPath, "https://from-env-var.example:6443")
	t.Setenv("KUBECONFIG", envPath)

	cfg, err := buildRESTConfig("")
	if err != nil {
		t.Fatalf("buildRESTConfig(\"\") error: %v", err)
	}
	if want := "https://from-env-var.example:6443"; cfg.Host != want {
		t.Errorf("cfg.Host = %q, want %q", cfg.Host, want)
	}
}

func TestStartHostLoadGoroutinesExitAfterStop(t *testing.T) {
	// startHostLoad's whole point is to keep the host's real CPU busy for as
	// long as the caller wants and no longer — a stop() that doesn't actually
	// stop the goroutines would silently burn the host's CPU (and battery)
	// indefinitely after the test run finishes.
	before := runtime.NumGoroutine()

	stop := startHostLoad(4)
	time.Sleep(20 * time.Millisecond)
	during := runtime.NumGoroutine()
	if during < before+4 {
		t.Fatalf("during load: NumGoroutine = %d, want at least %d (before=%d)", during, before+4, before)
	}

	stop()
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after > before+1 { // small slack for scheduling noise
		t.Fatalf("after stop: NumGoroutine = %d, want close to pre-load baseline %d", after, before)
	}
}

func TestStartHostLoadZeroWorkersIsANoop(t *testing.T) {
	before := runtime.NumGoroutine()

	stop := startHostLoad(0)
	time.Sleep(10 * time.Millisecond)
	if got := runtime.NumGoroutine(); got > before+1 {
		t.Fatalf("NumGoroutine = %d, want no new goroutines for 0 workers", got)
	}
	stop() // must not panic
}

func TestPodSpecWorkloadContainerMakesARealConnection(t *testing.T) {
	// ztunnel only attempts its on-demand identity lookup on a workload's
	// first real connection (see ambient-mode.md). A workload container that
	// only sleeps never exercises that path at all, so the burst pod's main
	// container must make a real request against the given target — a
	// dedicated, stays-warm service, not the k8s API server — not just idle.
	pod := podSpec("pod-a", "run-1", "sa-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080", initContainerModeNone)

	command := strings.Join(pod.Spec.Containers[0].Command, " ")
	if !strings.Contains(command, "wget") {
		t.Fatalf("workload container command %q doesn't appear to make a real HTTP request", command)
	}
	if !strings.Contains(command, "echo-target.ztunnel-diag.svc.cluster.local:8080") {
		t.Fatalf("workload container command %q doesn't target the given service", command)
	}
}

func TestPodSpecWorkloadConnectionOutlivesZtunnelsFiveSecondHold(t *testing.T) {
	// ztunnel acks the connection immediately and holds it open for up to 5s
	// waiting for routing info to arrive, then either forwards or drops it.
	// wget's own timeout must comfortably exceed not just that 5s hold but
	// the worst observed case of a connection that *does* still get forwarded
	// late (routing delays up to ~11s have been observed on a busy node,
	// see README) — too tight a budget makes wget give up first, which would
	// misreport a real (if slow) success as a ztunnel-caused failure.
	pod := podSpec("pod-a", "run-1", "sa-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080", initContainerModeNone)
	command := strings.Join(pod.Spec.Containers[0].Command, " ")

	if !strings.Contains(command, "-T 20") {
		t.Fatalf("command %q doesn't set a wget timeout comfortably past both ztunnel's 5s hold and observed late-forward delays", command)
	}
}

func TestPodSpecWorkloadCommandRecordsExitStatus(t *testing.T) {
	// The report needs to know whether the workload's own connection attempt
	// actually failed (ztunnel not informed of routing/identity in time),
	// not just infer it from ztunnel's own logs — so the command must record
	// wget's exit status somewhere durably readable back (its own container
	// log) after wget returns.
	pod := podSpec("pod-a", "run-1", "sa-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080", initContainerModeNone)
	command := strings.Join(pod.Spec.Containers[0].Command, " ")

	if !strings.Contains(command, exitMarkerPrefix+"$?") {
		t.Fatalf("command %q doesn't record wget's exit status after it returns", command)
	}
}

func TestConnectionFailed(t *testing.T) {
	tests := map[string]struct {
		log        string
		wantFailed bool
		wantKnown  bool
	}{
		"successful wget": {
			log:        "Connecting to echo-target...\nwget: OK\n" + exitMarkerPrefix + "0\n",
			wantFailed: false,
			wantKnown:  true,
		},
		"wget timed out waiting for ztunnel to route it": {
			log:        "Connecting to echo-target...\nwget: download timed out\n" + exitMarkerPrefix + "1\n",
			wantFailed: true,
			wantKnown:  true,
		},
		"marker never showed up (wget still running or log not yet flushed)": {
			log:        "Connecting to echo-target...\n",
			wantFailed: false,
			wantKnown:  false,
		},
		"marker present without trailing newline": {
			log:        exitMarkerPrefix + "4",
			wantFailed: true,
			wantKnown:  true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			failed, known := connectionFailed(tc.log)
			if failed != tc.wantFailed || known != tc.wantKnown {
				t.Errorf("connectionFailed(%q) = (%v, %v), want (%v, %v)", tc.log, failed, known, tc.wantFailed, tc.wantKnown)
			}
		})
	}
}

func TestResolveTargetToClusterIPResolvesServiceDNSName(t *testing.T) {
	// A workload hitting the target by its k8s Service DNS name makes ztunnel
	// resolve that DNS query through its own DNS proxy first, which needs the
	// *source* workload's identity to answer — an identity wait that a client
	// resolver like musl silently retries on timeout, independently of and
	// before the real outbound-connect-time hold this tool means to measure
	// (see README's "what actually gates the timeout"). Targeting the
	// Service's ClusterIP directly skips DNS so the outbound connect is the
	// first thing that can hit the hold.
	cs := fake.NewSimpleClientset(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "echo-target", Namespace: "ztunnel-diag"},
		Spec:       corev1.ServiceSpec{ClusterIP: "10.96.0.5"},
	})

	got := resolveTargetToClusterIP(context.Background(), cs, "ztunnel-diag", "echo-target.ztunnel-diag.svc.cluster.local:8080")

	if want := "10.96.0.5:8080"; got != want {
		t.Errorf("resolveTargetToClusterIP = %q, want %q", got, want)
	}
}

func TestResolveTargetToClusterIPLeavesRawIPAlone(t *testing.T) {
	cs := fake.NewSimpleClientset()

	got := resolveTargetToClusterIP(context.Background(), cs, "ztunnel-diag", "10.96.0.5:8080")

	if want := "10.96.0.5:8080"; got != want {
		t.Errorf("resolveTargetToClusterIP = %q, want %q", got, want)
	}
}

func TestResolveTargetToClusterIPFallsBackWhenServiceNotFound(t *testing.T) {
	// A --target pointing outside this namespace/cluster (or a typo) must not
	// make the tool fail outright — fall back to the given target unchanged
	// and let the workload's own DNS resolution handle it as before.
	cs := fake.NewSimpleClientset()

	got := resolveTargetToClusterIP(context.Background(), cs, "ztunnel-diag", "echo-target.ztunnel-diag.svc.cluster.local:8080")

	if want := "echo-target.ztunnel-diag.svc.cluster.local:8080"; got != want {
		t.Errorf("resolveTargetToClusterIP = %q, want %q", got, want)
	}
}

func TestPodSpecOmitsInitContainerByDefault(t *testing.T) {
	pod := podSpec("pod-a", "run-1", "sa-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080", initContainerModeNone)

	if len(pod.Spec.InitContainers) != 0 {
		t.Fatalf("InitContainers = %v, want none", pod.Spec.InitContainers)
	}
}

func TestPodSpecNoopInitContainerHasNoProbeLogic(t *testing.T) {
	// The "noop" mode exists specifically to isolate whether adding *any*
	// init container — independent of what it does — accounts for the
	// mitigation's effect (extra container-start overhead under a saturated
	// node). It must not touch the network or record an outcome; if it did,
	// it wouldn't be a control.
	pod := podSpec("pod-a", "run-1", "sa-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080", initContainerModeNoop)

	if len(pod.Spec.InitContainers) != 1 {
		t.Fatalf("InitContainers = %v, want exactly 1", pod.Spec.InitContainers)
	}
	command := strings.Join(pod.Spec.InitContainers[0].Command, " ")
	if strings.Contains(command, "KUBERNETES_SERVICE_HOST") || strings.Contains(command, "wget") {
		t.Fatalf("noop init container command %q isn't a no-op — it still probes something", command)
	}
}

func TestPodSpecInitContainerProbesAPIServerNotDNS(t *testing.T) {
	// The mitigation must not depend on any test-specific service (constraint:
	// only the k8s API server, control plane, and node are known upfront) and
	// must not go through DNS — a DNS lookup can get silently retried by the
	// client resolver and succeed without ever exercising the real
	// outbound-connect-time hold this probe means to wait out (see README).
	// $KUBERNETES_SERVICE_HOST/_PORT are injected into every pod by kubelet,
	// no Service lookup or DNS required.
	pod := podSpec("pod-a", "run-1", "sa-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080", initContainerModeProbe)

	if len(pod.Spec.InitContainers) != 1 {
		t.Fatalf("InitContainers = %v, want exactly 1", pod.Spec.InitContainers)
	}
	command := strings.Join(pod.Spec.InitContainers[0].Command, " ")
	if !strings.Contains(command, "$KUBERNETES_SERVICE_HOST") || !strings.Contains(command, "$KUBERNETES_SERVICE_PORT") {
		t.Fatalf("init container command %q doesn't probe the API server via its injected env vars", command)
	}
}

func TestPodSpecInitContainerRequiresARealHTTPResponseNotJustALocalAccept(t *testing.T) {
	// nc -z only proves the TCP handshake completed against ztunnel's local
	// interception point — which happens instantly regardless of whether
	// ztunnel has resolved this pod's identity, since the identity check and
	// the decision to forward/drop happen asynchronously, after the client
	// already sees "connected". A probe using -z reports ready immediately on
	// every attempt and never actually waits for anything (confirmed against
	// the real cluster: attempts=1 elapsed=0s on every pod, while ztunnel's
	// own logs showed the real 5s wait-then-drop still happening in the
	// background).
	//
	// A raw byte sent over nc doesn't work either: the API server's Go TLS
	// stack silently closes the connection on malformed input instead of
	// sending an alert back, so "any response bytes" reads as 0 even once
	// genuinely forwarded (confirmed against the real cluster). wget's HTTPS
	// support gets a real HTTP response line even on a 403 (confirmed: "server
	// returned error: HTTP/1.1 403 Forbidden") — that response line is the
	// proof of a genuine forward a bare connect can't give.
	pod := podSpec("pod-a", "run-1", "sa-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080", initContainerModeProbe)

	command := strings.Join(pod.Spec.InitContainers[0].Command, " ")
	if strings.Contains(command, "nc -z") || strings.Contains(command, "-z ") {
		t.Fatalf("init container command %q uses nc's zero-I/O mode, which succeeds on a local accept without proving ztunnel forwarded anything", command)
	}
	if !strings.Contains(command, "wget") || !strings.Contains(command, "HTTP/") {
		t.Fatalf("init container command %q doesn't check for a real HTTP response line", command)
	}
}

func TestPodSpecInitContainerRecordsItsOwnOutcome(t *testing.T) {
	// RoutingDelay can't measure the workload's own wait when an init
	// container is present without one (see ztlog.RoutingDelay's
	// targetService scoping) — but there's still no way to tell, from the
	// workload's exit code alone, whether the mitigation itself succeeded
	// early or rode its retry budget all the way to exhaustion. The init
	// container must record that outcome in its own log.
	pod := podSpec("pod-a", "run-1", "sa-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080", initContainerModeProbe)

	command := strings.Join(pod.Spec.InitContainers[0].Command, " ")
	if !strings.Contains(command, initOutcomeMarkerPrefix) {
		t.Fatalf("init container command %q doesn't record its own outcome", command)
	}
}

func TestPodSpecInitContainerFailsOpen(t *testing.T) {
	// A pod stuck on a node where identity never lands must still start its
	// main container eventually (the mitigation must not itself introduce a
	// way for a pod to hang forever) — so the init container's command must
	// exit 0 unconditionally, not propagate the probe loop's own exit status.
	pod := podSpec("pod-a", "run-1", "sa-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080", initContainerModeProbe)

	command := pod.Spec.InitContainers[0].Command
	if len(command) == 0 || !strings.HasSuffix(strings.TrimSpace(command[len(command)-1]), "exit 0") {
		t.Fatalf("init container command %q doesn't unconditionally exit 0", command)
	}
}

func TestInitContainerOutcome(t *testing.T) {
	tests := map[string]struct {
		log          string
		wantReady    bool
		wantKnown    bool
		wantAttempts int
		wantElapsed  time.Duration
	}{
		"succeeded on the third attempt": {
			log:          initOutcomeMarkerPrefix + "ready attempts=3 elapsed=6s\n",
			wantReady:    true,
			wantKnown:    true,
			wantAttempts: 3,
			wantElapsed:  6 * time.Second,
		},
		"exhausted its retry budget": {
			log:          initOutcomeMarkerPrefix + "exhausted attempts=15 elapsed=29s\n",
			wantReady:    false,
			wantKnown:    true,
			wantAttempts: 15,
			wantElapsed:  29 * time.Second,
		},
		"marker never showed up": {
			log:       "some other output\n",
			wantReady: false,
			wantKnown: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ready, known, attempts, elapsed := initContainerOutcome(tc.log)
			if ready != tc.wantReady || known != tc.wantKnown || attempts != tc.wantAttempts || elapsed != tc.wantElapsed {
				t.Errorf("initContainerOutcome(%q) = (%v, %v, %d, %v), want (%v, %v, %d, %v)",
					tc.log, ready, known, attempts, elapsed, tc.wantReady, tc.wantKnown, tc.wantAttempts, tc.wantElapsed)
			}
		})
	}
}

func TestPodSpecUsesGivenServiceAccount(t *testing.T) {
	// istio's workload identity is per (namespace, ServiceAccount), not per
	// pod. Every burst pod needs its own ServiceAccount, otherwise after the
	// first pod istiod has nothing new to push and the race this tool exists
	// to measure never gets exercised for pods 2..N.
	pod := podSpec("pod-a", "run-1", "sa-for-pod-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080", initContainerModeNone)

	if got := pod.Spec.ServiceAccountName; got != "sa-for-pod-a" {
		t.Fatalf("ServiceAccountName = %q, want %q", got, "sa-for-pod-a")
	}
}

func TestPodSpecPinsToTargetNode(t *testing.T) {
	// The whole point of the test is saturating one specific node's pod
	// capacity, so every burst pod must be pinned there.
	pod := podSpec("pod-a", "run-1", "sa-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080", initContainerModeNone)

	if got := pod.Spec.NodeSelector["kubernetes.io/hostname"]; got != "ztunnel-diag-m02" {
		t.Fatalf("NodeSelector[kubernetes.io/hostname] = %q, want %q", got, "ztunnel-diag-m02")
	}
}
