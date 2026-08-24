package ztlog

import (
	"testing"
	"time"
)

// Real lines captured from a RUST_LOG=debug ztunnel pod (see
// hack/scale-node-burst.sh runs) for one successfully-routed connection.
var realDebugTrace = []string{
	`2026-08-19T22:32:56.957379Z	debug	inpod::workloadmanager	received message: AddWorkload(...)`,
	`2026-08-19T22:32:56.957383Z	info	inpod::statemanager	pod received, starting proxy	uid="b43879bc" name="ztunnel-diag-1787178776283731000-0" namespace="ztunnel-diag"`,
	`2026-08-19T22:32:57.938475Z	debug	state:lookup{src=10.244.0.7:41453 query=AAAA name=kubernetes.default.svc.cluster.local.ztunnel-diag.svc.cluster.local.}	wait for workload	wl=ztunnel-diag-1787178776283731000-0.ztunnel-diag (ztunnel-diag-1787178776283731000-0)`,
	`2026-08-19T22:32:58.367675Z	debug	dns	src.workload="ztunnel-diag-1787178776283731000-0" src.namespace="ztunnel-diag" query="A" domain="kubernetes.default.svc.cluster.local.ztunnel-diag.svc.cluster.local." result="success" endpoints=1`,
	`2026-08-19T22:32:58.367972Z	debug	access	connection opened	src.addr=10.244.0.7:40461 src.workload="ztunnel-diag-1787178776283731000-0" src.namespace="ztunnel-diag" dst.addr=172.17.0.3:8443 dst.service="kubernetes.default.svc.cluster.local" dst.workload="kubernetes" dst.namespace="default" direction="outbound"`,
	`2026-08-19T22:33:05.939619Z	info	access	connection complete	src.addr=10.244.0.7:40461 src.workload="ztunnel-diag-1787178776283731000-0" src.namespace="ztunnel-diag" dst.addr=172.17.0.3:8443 dst.service="kubernetes.default.svc.cluster.local" dst.workload="kubernetes" dst.namespace="default" direction="outbound" bytes_sent=0 bytes_recv=0 duration="7571ms"`,
}

func TestMatchesTimeoutForPod(t *testing.T) {
	tests := map[string]struct {
		line string
		pod  string
		want bool
	}{
		"matching pod name and timeout phrase (real ztunnel format)": {
			line: `2026-08-19T21:13:40.121023Z	warn	state	timed out waiting for workload 'ztunnel-diag-run1-180.ztunnel-diag (ztunnel-diag-run1-180)' from xds	`,
			pod:  "ztunnel-diag-run1-180",
			want: true,
		},
		"timeout phrase, different pod": {
			line: `2026-08-19T21:13:41.300917Z	warn	state	timed out waiting for workload 'ztunnel-diag-run1-196.ztunnel-diag (ztunnel-diag-run1-196)' from xds	`,
			pod:  "ztunnel-diag-run1-180",
			want: false,
		},
		"matching pod, no timeout phrase": {
			line: `2026-08-19T21:13:41.300917Z	info	access	connection complete	src.workload="ztunnel-diag-run1-180"`,
			pod:  "ztunnel-diag-run1-180",
			want: false,
		},
		"empty pod name never matches": {
			line: `2026-08-19T21:13:40.121023Z	warn	state	timed out waiting for workload 'ztunnel-diag-run1-180.ztunnel-diag (ztunnel-diag-run1-180)' from xds	`,
			pod:  "",
			want: false,
		},
		"pod name that is a prefix of another pod's name doesn't false-positive": {
			line: `2026-08-19T21:13:40.121023Z	warn	state	timed out waiting for workload 'ztunnel-diag-run1-180.ztunnel-diag (ztunnel-diag-run1-180)' from xds	`,
			pod:  "ztunnel-diag-run1-18",
			want: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := MatchesTimeoutForPod(tc.line, tc.pod)
			if got != tc.want {
				t.Errorf("MatchesTimeoutForPod(%q, %q) = %v, want %v", tc.line, tc.pod, got, tc.want)
			}
		})
	}
}

const realDebugTraceTarget = "kubernetes.default.svc.cluster.local"

func TestRoutingDelay(t *testing.T) {
	got, ok := RoutingDelay(realDebugTrace, "ztunnel-diag-1787178776283731000-0", "ztunnel-diag", realDebugTraceTarget)
	if !ok {
		t.Fatal("RoutingDelay: ok = false, want true")
	}
	// 22:32:57.938475 (wait for workload) -> 22:32:58.367972 (connection opened)
	want := 429497 * time.Microsecond
	if got != want {
		t.Errorf("RoutingDelay = %v, want %v", got, want)
	}
}

func TestRoutingDelayIgnoresConnectionCompleteDuration(t *testing.T) {
	// The access log's "connection complete ... duration=" field is the
	// whole connection lifetime (however long the client held the socket
	// open), not ztunnel's internal wait — RoutingDelay must not confuse the
	// two.
	got, ok := RoutingDelay(realDebugTrace, "ztunnel-diag-1787178776283731000-0", "ztunnel-diag", realDebugTraceTarget)
	if !ok {
		t.Fatal("RoutingDelay: ok = false, want true")
	}
	if got > time.Second {
		t.Errorf("RoutingDelay = %v, looks like it picked up the connection-complete duration instead of the wait-for-workload gap", got)
	}
}

func TestRoutingDelayNoMatchReturnsFalse(t *testing.T) {
	if _, ok := RoutingDelay(realDebugTrace, "some-other-pod", "ztunnel-diag", realDebugTraceTarget); ok {
		t.Error("RoutingDelay: ok = true for a pod not present in the logs, want false")
	}
}

func TestRoutingDelayRequiresBothMarkers(t *testing.T) {
	waitOnly := realDebugTrace[:3] // includes "wait for workload" but not "connection opened"
	if _, ok := RoutingDelay(waitOnly, "ztunnel-diag-1787178776283731000-0", "ztunnel-diag", realDebugTraceTarget); ok {
		t.Error("RoutingDelay: ok = true with no \"connection opened\" line, want false")
	}
}

func TestIdentityReadyAtIgnoresDestination(t *testing.T) {
	// Unlike RoutingDelay, IdentityReadyAt deliberately does NOT scope by
	// dst.service: ztunnel's identity gate is per source workload, not per
	// destination, so the first successful connection to ANYWHERE (e.g. a
	// mitigation init container's own probe to the API server) is real proof
	// identity had landed by that moment — an upper bound on the true
	// readiness time, since it can only be detected once something tries.
	pod, ns := "ztunnel-diag-run1-0", "ztunnel-diag"
	trace := []string{
		`2026-08-19T21:00:01.000000Z	debug	access	connection opened	src.workload="` + pod + `" dst.service="kubernetes.default.svc.cluster.local"`,
		`2026-08-19T21:00:06.000000Z	debug	access	connection opened	src.workload="` + pod + `" dst.service="echo-target.ztunnel-diag.svc.cluster.local"`,
	}

	got, ok := IdentityReadyAt(trace, pod, ns)
	if !ok {
		t.Fatal("IdentityReadyAt: ok = false, want true")
	}
	want, _ := time.Parse(time.RFC3339Nano, "2026-08-19T21:00:01.000000Z")
	if !got.Equal(want) {
		t.Errorf("IdentityReadyAt = %v, want %v (should pick the earlier connection to any destination)", got, want)
	}
}

func TestIdentityReadyAtNoMatchReturnsFalse(t *testing.T) {
	if _, ok := IdentityReadyAt(realDebugTrace, "some-other-pod", "ztunnel-diag"); ok {
		t.Error("IdentityReadyAt: ok = true for a pod not present in the logs, want false")
	}
}

func TestLastPreSuccessFailureAt(t *testing.T) {
	// Real line format captured from a saturated-node run: the failure
	// embeds the pod's identity the same way the timeout warning does
	// (`<pod>.<namespace> (<pod>)`), just inside a longer error= string
	// rather than a quoted phrase.
	pod, ns := "ztunnel-diag-1787536400097448000-0", "ztunnel-diag"
	trace := []string{
		`2026-08-24T01:53:53.071068Z	debug	state:proxy{wl=` + pod + `.` + ns + `}:outbound{id=1}	wait for workload	wl=` + pod + `.` + ns + ` (` + pod + `)`,
		`2026-08-24T01:53:58.072727Z	warn	access	connection failed	src.addr=10.244.1.104:35211 dst.addr=10.96.0.1:443 direction="outbound" error="failed to fetch information about local workload: ` + pod + `.` + ns + ` (` + pod + `)"`,
		`2026-08-24T01:54:03.430460Z	debug	access	connection opened	src.workload="` + pod + `" dst.service="echo-target.ztunnel-diag.svc.cluster.local"`,
	}

	got, ok := LastPreSuccessFailureAt(trace, pod, ns)
	if !ok {
		t.Fatal("LastPreSuccessFailureAt: ok = false, want true")
	}
	want, _ := time.Parse(time.RFC3339Nano, "2026-08-24T01:53:58.072727Z")
	if !got.Equal(want) {
		t.Errorf("LastPreSuccessFailureAt = %v, want %v", got, want)
	}
}

func TestLastPreSuccessFailureAtNoFailuresReturnsFalse(t *testing.T) {
	if _, ok := LastPreSuccessFailureAt(realDebugTrace, "ztunnel-diag-1787178776283731000-0", "ztunnel-diag"); ok {
		t.Error("LastPreSuccessFailureAt: ok = true with no failure lines, want false")
	}
}

func TestLastPreSuccessFailureAtDoesNotFalsePositiveOnPrefixCollision(t *testing.T) {
	// pod-6's own failure line must not match a lookup for pod-60 or vice
	// versa — same class of bug MatchesTimeoutForPod already guards against.
	trace := []string{
		`2026-08-24T01:53:58.072727Z	warn	access	connection failed	src.addr=10.244.1.104:35211 dst.addr=10.96.0.1:443 direction="outbound" error="failed to fetch information about local workload: ztunnel-diag-60.ztunnel-diag (ztunnel-diag-60)"`,
	}
	if _, ok := LastPreSuccessFailureAt(trace, "ztunnel-diag-6", "ztunnel-diag"); ok {
		t.Error("LastPreSuccessFailureAt: ok = true, want false (pod-6 matched pod-60's failure line)")
	}
}

func TestRoutingDelaySkipsConnectionsToOtherDestinations(t *testing.T) {
	// A mitigation init container's own probe connection (see cmd/ztunnel-diag)
	// shares the workload's pod name/identity from ztunnel's point of view —
	// the only thing that tells the two connections apart in the log is
	// dst.service. RoutingDelay must measure the real target's connection,
	// not whichever "connection opened" line happens to come first.
	pod, ns := "ztunnel-diag-run1-0", "ztunnel-diag"
	trace := []string{
		`2026-08-19T21:00:00.000000Z	debug	state:lookup{}	wait for workload	wl=` + pod + `.` + ns,
		`2026-08-19T21:00:01.000000Z	debug	access	connection opened	src.workload="` + pod + `" dst.service="kubernetes.default.svc.cluster.local"`,
		`2026-08-19T21:00:06.000000Z	debug	access	connection opened	src.workload="` + pod + `" dst.service="echo-target.ztunnel-diag.svc.cluster.local"`,
	}

	got, ok := RoutingDelay(trace, pod, ns, "echo-target.ztunnel-diag.svc.cluster.local")
	if !ok {
		t.Fatal("RoutingDelay: ok = false, want true")
	}
	if want := 6 * time.Second; got != want {
		t.Errorf("RoutingDelay = %v, want %v (picked up the probe's earlier connection instead of the real target's)", got, want)
	}
}
