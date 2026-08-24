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

func TestComputeConnectionFailedCountOnlyOverKnownPods(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []PodEvent{
		{Name: "a", IPAssignedAt: base, IPPatchedAtAPI: base, ConnectionFailed: true, ConnectionFailedKnown: true},
		{Name: "b", IPAssignedAt: base, IPPatchedAtAPI: base, ConnectionFailed: false, ConnectionFailedKnown: true},
		{Name: "c", IPAssignedAt: base, IPPatchedAtAPI: base, ConnectionFailed: false, ConnectionFailedKnown: false},
	}

	got := Compute(events)

	if got.ConnectionFailedKnownCount != 2 {
		t.Errorf("ConnectionFailedKnownCount = %d, want 2", got.ConnectionFailedKnownCount)
	}
	if got.ConnectionFailedCount != 1 {
		t.Errorf("ConnectionFailedCount = %d, want 1", got.ConnectionFailedCount)
	}
}

func TestComputeInitContainerOutcomeOnlyOverKnownPods(t *testing.T) {
	// A pod's workload succeeding or failing doesn't say whether the
	// mitigation init container got there early or rode its retry budget to
	// exhaustion — that's a separate signal, only present when
	// --init-container was used (InitContainerOutcomeKnown false otherwise).
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []PodEvent{
		{Name: "a", IPAssignedAt: base, IPPatchedAtAPI: base, InitContainerOutcomeKnown: true, InitContainerReady: true, InitContainerElapsed: 3 * time.Second},
		{Name: "b", IPAssignedAt: base, IPPatchedAtAPI: base, InitContainerOutcomeKnown: true, InitContainerReady: false, InitContainerElapsed: 29 * time.Second},
		{Name: "c", IPAssignedAt: base, IPPatchedAtAPI: base, InitContainerOutcomeKnown: false},
	}

	got := Compute(events)

	if got.InitContainerKnownCount != 2 {
		t.Errorf("InitContainerKnownCount = %d, want 2", got.InitContainerKnownCount)
	}
	if got.InitContainerReadyCount != 1 {
		t.Errorf("InitContainerReadyCount = %d, want 1", got.InitContainerReadyCount)
	}
	if got.InitContainerExhaustedCount != 1 {
		t.Errorf("InitContainerExhaustedCount = %d, want 1", got.InitContainerExhaustedCount)
	}
	if want := 16 * time.Second; got.MeanInitContainerElapsed != want {
		t.Errorf("MeanInitContainerElapsed = %v, want %v", got.MeanInitContainerElapsed, want)
	}
	if want := 29 * time.Second; got.MaxInitContainerElapsed != want {
		t.Errorf("MaxInitContainerElapsed = %v, want %v", got.MaxInitContainerElapsed, want)
	}
}

func TestComputeTimeoutVsFailureBreakdown(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []PodEvent{
		// expected majority: failed and ztunnel logged the timeout.
		{Name: "a", IPAssignedAt: base, IPPatchedAtAPI: base, ZtunnelTimeout: true, ConnectionFailed: true, ConnectionFailedKnown: true},
		// expected majority: succeeded and no timeout logged.
		{Name: "b", IPAssignedAt: base, IPPatchedAtAPI: base, ZtunnelTimeout: false, ConnectionFailed: false, ConnectionFailedKnown: true},
		// mismatch: failed for some other reason.
		{Name: "c", IPAssignedAt: base, IPPatchedAtAPI: base, ZtunnelTimeout: false, ConnectionFailed: true, ConnectionFailedKnown: true},
		// mismatch: ztunnel warned but the connection still succeeded.
		{Name: "d", IPAssignedAt: base, IPPatchedAtAPI: base, ZtunnelTimeout: true, ConnectionFailed: false, ConnectionFailedKnown: true},
		// exit marker never seen: excluded entirely.
		{Name: "e", IPAssignedAt: base, IPPatchedAtAPI: base, ZtunnelTimeout: true, ConnectionFailed: false, ConnectionFailedKnown: false},
	}

	got := Compute(events)

	if got.TimeoutVsFailureComparableCount != 4 {
		t.Errorf("TimeoutVsFailureComparableCount = %d, want 4", got.TimeoutVsFailureComparableCount)
	}
	if got.FailedAndTimedOut != 1 {
		t.Errorf("FailedAndTimedOut = %d, want 1", got.FailedAndTimedOut)
	}
	if got.OKNotTimedOut != 1 {
		t.Errorf("OKNotTimedOut = %d, want 1", got.OKNotTimedOut)
	}
	if got.FailedNotTimedOut != 1 {
		t.Errorf("FailedNotTimedOut = %d, want 1", got.FailedNotTimedOut)
	}
	if got.OKButTimedOut != 1 {
		t.Errorf("OKButTimedOut = %d, want 1", got.OKButTimedOut)
	}
}

func TestComputeMitigationNecessityClassification(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []PodEvent{
		// upper bound <= 5s: proven unnecessary (this is what every `none`-mode
		// success looks like, trivially, since it succeeded on one shot).
		{
			Name: "a", IPAssignedAt: base, IPPatchedAtAPI: base,
			ConnectionFailed: false, ConnectionFailedKnown: true,
			CounterfactualWaitUpperBound: 3 * time.Second, CounterfactualWaitUpperBoundKnown: true,
		},
		// upper bound > 5s, lower bound (from probe's own retries) also > 5s:
		// proven necessary.
		{
			Name: "b", IPAssignedAt: base, IPPatchedAtAPI: base,
			ConnectionFailed: false, ConnectionFailedKnown: true,
			CounterfactualWaitUpperBound: 12 * time.Second, CounterfactualWaitUpperBoundKnown: true,
			CounterfactualWaitLowerBound: 6 * time.Second, CounterfactualWaitLowerBoundKnown: true,
		},
		// upper bound > 5s, no lower bound at all (typical noop case): ambiguous.
		{
			Name: "c", IPAssignedAt: base, IPPatchedAtAPI: base,
			ConnectionFailed: false, ConnectionFailedKnown: true,
			CounterfactualWaitUpperBound: 8 * time.Second, CounterfactualWaitUpperBoundKnown: true,
		},
		// upper bound > 5s, lower bound known but still <= 5s: not enough to
		// prove necessity either — still ambiguous.
		{
			Name: "d", IPAssignedAt: base, IPPatchedAtAPI: base,
			ConnectionFailed: false, ConnectionFailedKnown: true,
			CounterfactualWaitUpperBound: 8 * time.Second, CounterfactualWaitUpperBoundKnown: true,
			CounterfactualWaitLowerBound: 2 * time.Second, CounterfactualWaitLowerBoundKnown: true,
		},
		// failed pod: excluded entirely, the causal question doesn't apply.
		{
			Name: "e", IPAssignedAt: base, IPPatchedAtAPI: base,
			ConnectionFailed: true, ConnectionFailedKnown: true,
			CounterfactualWaitUpperBound: 20 * time.Second, CounterfactualWaitUpperBoundKnown: true,
		},
		// no counterfactual data at all (e.g. RUST_LOG=debug wasn't set):
		// excluded, not counted as ambiguous.
		{
			Name: "f", IPAssignedAt: base, IPPatchedAtAPI: base,
			ConnectionFailed: false, ConnectionFailedKnown: true,
		},
	}

	got := Compute(events)

	if got.MitigationComparableCount != 4 {
		t.Errorf("MitigationComparableCount = %d, want 4", got.MitigationComparableCount)
	}
	if got.MitigationDefinitelyUnnecessary != 1 {
		t.Errorf("MitigationDefinitelyUnnecessary = %d, want 1", got.MitigationDefinitelyUnnecessary)
	}
	if got.MitigationDefinitelyNecessary != 1 {
		t.Errorf("MitigationDefinitelyNecessary = %d, want 1", got.MitigationDefinitelyNecessary)
	}
	if got.MitigationAmbiguous != 2 {
		t.Errorf("MitigationAmbiguous = %d, want 2", got.MitigationAmbiguous)
	}
}
