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

func TestRoutingDelay(t *testing.T) {
	got, ok := RoutingDelay(realDebugTrace, "ztunnel-diag-1787178776283731000-0", "ztunnel-diag")
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
	got, ok := RoutingDelay(realDebugTrace, "ztunnel-diag-1787178776283731000-0", "ztunnel-diag")
	if !ok {
		t.Fatal("RoutingDelay: ok = false, want true")
	}
	if got > time.Second {
		t.Errorf("RoutingDelay = %v, looks like it picked up the connection-complete duration instead of the wait-for-workload gap", got)
	}
}

func TestRoutingDelayNoMatchReturnsFalse(t *testing.T) {
	if _, ok := RoutingDelay(realDebugTrace, "some-other-pod", "ztunnel-diag"); ok {
		t.Error("RoutingDelay: ok = true for a pod not present in the logs, want false")
	}
}

func TestRoutingDelayRequiresBothMarkers(t *testing.T) {
	waitOnly := realDebugTrace[:3] // includes "wait for workload" but not "connection opened"
	if _, ok := RoutingDelay(waitOnly, "ztunnel-diag-1787178776283731000-0", "ztunnel-diag"); ok {
		t.Error("RoutingDelay: ok = true with no \"connection opened\" line, want false")
	}
}
