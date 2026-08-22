package main

import (
	"runtime"
	"strings"
	"testing"
	"time"
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
	// waiting for routing info to arrive, only then forwarding or dropping
	// it. wget's own request timeout must comfortably exceed that 5s hold,
	// or it'll give up and disconnect before the hold could ever resolve —
	// masking a real timeout as "probe gave up," not "ztunnel timed out."
	pod := podSpec("pod-a", "run-1", "sa-a", "ztunnel-diag-m02", "echo-target.ztunnel-diag.svc.cluster.local:8080")
	command := strings.Join(pod.Spec.Containers[0].Command, " ")

	if !strings.Contains(command, "-T 10") {
		t.Fatalf("command %q doesn't set a wget timeout comfortably past ztunnel's 5s hold", command)
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
