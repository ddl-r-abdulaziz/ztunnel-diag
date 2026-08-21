package report

import (
	"testing"
	"time"
)

func mustEvents(base time.Time, patchDelaysSeconds []float64, timedOut []bool) []PodEvent {
	events := make([]PodEvent, len(patchDelaysSeconds))
	for i, delay := range patchDelaysSeconds {
		events[i] = PodEvent{
			Name:           "pod-" + string(rune('a'+i)),
			IPAssignedAt:   base,
			IPPatchedAtAPI: base.Add(time.Duration(delay * float64(time.Second))),
			ZtunnelTimeout: timedOut[i],
		}
	}
	return events
}

func TestCompute(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := map[string]struct {
		events       []PodEvent
		wantCount    int
		wantTimeouts int
		wantMaxSecs  float64
		wantMeanSecs float64
	}{
		"single fast pod, no timeout": {
			events:       mustEvents(base, []float64{0.2}, []bool{false}),
			wantCount:    1,
			wantTimeouts: 0,
			wantMaxSecs:  0.2,
			wantMeanSecs: 0.2,
		},
		"burst with one slow patch that times out": {
			events:       mustEvents(base, []float64{0.5, 7.0, 1.0}, []bool{false, true, false}),
			wantCount:    3,
			wantTimeouts: 1,
			wantMaxSecs:  7.0,
			wantMeanSecs: (0.5 + 7.0 + 1.0) / 3,
		},
		"empty burst": {
			events:       nil,
			wantCount:    0,
			wantTimeouts: 0,
			wantMaxSecs:  0,
			wantMeanSecs: 0,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := Compute(tc.events)

			if got.Count != tc.wantCount {
				t.Errorf("Count = %d, want %d", got.Count, tc.wantCount)
			}
			if got.TimeoutCount != tc.wantTimeouts {
				t.Errorf("TimeoutCount = %d, want %d", got.TimeoutCount, tc.wantTimeouts)
			}
			if got.MaxPatchLatency.Seconds() != tc.wantMaxSecs {
				t.Errorf("MaxPatchLatency = %v, want %vs", got.MaxPatchLatency, tc.wantMaxSecs)
			}
			if diff := got.MeanPatchLatency.Seconds() - tc.wantMeanSecs; diff > 1e-9 || diff < -1e-9 {
				t.Errorf("MeanPatchLatency = %v, want %vs", got.MeanPatchLatency, tc.wantMeanSecs)
			}
		})
	}
}

func TestComputePreservesPerPodLatency(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	events := mustEvents(base, []float64{0.5, 7.0}, []bool{false, true})

	got := Compute(events)

	if len(got.Pods) != 2 {
		t.Fatalf("len(Pods) = %d, want 2", len(got.Pods))
	}
	if got.Pods[1].PatchLatency.Seconds() != 7.0 {
		t.Errorf("Pods[1].PatchLatency = %v, want 7s", got.Pods[1].PatchLatency)
	}
	if !got.Pods[1].ZtunnelTimeout {
		t.Errorf("Pods[1].ZtunnelTimeout = false, want true")
	}
}

func TestComputeAggregatesRoutingDelayOnlyOverKnownPods(t *testing.T) {
	// RoutingDelay requires RUST_LOG=debug on ztunnel — when it wasn't
	// enabled (or a pod's connection never opened), RoutingDelayKnown is
	// false and that pod must not silently drag the mean/max toward zero.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []PodEvent{
		{Name: "a", IPAssignedAt: base, IPPatchedAtAPI: base, RoutingDelay: 200 * time.Millisecond, RoutingDelayKnown: true},
		{Name: "b", IPAssignedAt: base, IPPatchedAtAPI: base, RoutingDelay: 0, RoutingDelayKnown: false},
		{Name: "c", IPAssignedAt: base, IPPatchedAtAPI: base, RoutingDelay: 600 * time.Millisecond, RoutingDelayKnown: true},
	}

	got := Compute(events)

	if got.RoutingDelayCount != 2 {
		t.Fatalf("RoutingDelayCount = %d, want 2", got.RoutingDelayCount)
	}
	if want := 400 * time.Millisecond; got.MeanRoutingDelay != want {
		t.Errorf("MeanRoutingDelay = %v, want %v", got.MeanRoutingDelay, want)
	}
	if want := 600 * time.Millisecond; got.MaxRoutingDelay != want {
		t.Errorf("MaxRoutingDelay = %v, want %v", got.MaxRoutingDelay, want)
	}
}

func TestComputeRoutingDelayAllUnknown(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []PodEvent{
		{Name: "a", IPAssignedAt: base, IPPatchedAtAPI: base},
	}

	got := Compute(events)

	if got.RoutingDelayCount != 0 {
		t.Errorf("RoutingDelayCount = %d, want 0", got.RoutingDelayCount)
	}
	if got.MeanRoutingDelay != 0 || got.MaxRoutingDelay != 0 {
		t.Errorf("Mean/MaxRoutingDelay = %v/%v, want 0/0", got.MeanRoutingDelay, got.MaxRoutingDelay)
	}
}
