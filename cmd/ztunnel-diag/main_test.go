package main

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

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
	pod := podSpec("pod-a", "run-1", "sa-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080")

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
	pod := podSpec("pod-a", "run-1", "sa-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080")
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
	pod := podSpec("pod-a", "run-1", "sa-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080")
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

func TestPodSpecUsesGivenServiceAccount(t *testing.T) {
	// istio's workload identity is per (namespace, ServiceAccount), not per
	// pod. Every burst pod needs its own ServiceAccount, otherwise after the
	// first pod istiod has nothing new to push and the race this tool exists
	// to measure never gets exercised for pods 2..N.
	pod := podSpec("pod-a", "run-1", "sa-for-pod-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080")

	if got := pod.Spec.ServiceAccountName; got != "sa-for-pod-a" {
		t.Fatalf("ServiceAccountName = %q, want %q", got, "sa-for-pod-a")
	}
}

func TestPodSpecPinsToTargetNode(t *testing.T) {
	// The whole point of the test is saturating one specific node's pod
	// capacity, so every burst pod must be pinned there.
	pod := podSpec("pod-a", "run-1", "sa-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080")

	if got := pod.Spec.NodeSelector["kubernetes.io/hostname"]; got != "ztunnel-diag-m02" {
		t.Fatalf("NodeSelector[kubernetes.io/hostname] = %q, want %q", got, "ztunnel-diag-m02")
	}
}
