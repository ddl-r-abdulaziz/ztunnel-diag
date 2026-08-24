// Package report computes ztunnel-startup-race statistics from observed pod
// lifecycle timestamps: how long kubelet took to patch a pod's IP onto its
// API-server status, and whether ztunnel timed out waiting for that workload's
// identity as a result.
package report

import (
	"slices"
	"time"
)

// PodEvent captures the timestamps needed to measure one pod's race between
// kubelet reporting its IP and ztunnel needing that IP's identity.
type PodEvent struct {
	Name           string
	IPAssignedAt   time.Time // when the sandbox/CNI assigned the IP locally
	IPPatchedAtAPI time.Time // when kubelet's status patch landed on the API server
	ZtunnelTimeout bool      // whether ztunnel logged a "timed out waiting for workload" for this pod

	// InitContainerMode is this pod's actual init container mode ("", "noop"
	// or "probe")
	InitContainerMode string

	// RoutingDelay is how long ztunnel itself took, from first needing this
	// workload's identity to actually routing its connection, as opposed to
	// PatchLatency below. Only meaningful when RoutingDelayKnown is true
	RoutingDelay      time.Duration
	RoutingDelayKnown bool

	// ConnectionFailed is whether the workload pod's own connection attempt
	// actually failed.
	// Only meaningful when ConnectionFailedKnown is true.
	ConnectionFailed      bool
	ConnectionFailedKnown bool

	// InitContainer* report the mitigation init container's own outcome —
	// whether its probe loop found ztunnel ready or exhausted its retry
	// budget — independent of whether the workload's own connection
	// succeeded. Only meaningful when InitContainerOutcomeKnown is true
	// (i.e. --init-container was used).
	InitContainerOutcomeKnown bool
	InitContainerReady        bool
	InitContainerAttempts     int
	InitContainerElapsed      time.Duration

	// CounterfactualWaitUpperBound is how long this pod's connection would
	// have had to wait for identity had it been attempted at the moment its
	// first container (init, if any, else workload) actually started, i.e.
	// under no mitigation at all. It's an upper bound, not an exact value.
	CounterfactualWaitUpperBound      time.Duration
	CounterfactualWaitUpperBoundKnown bool

	// CounterfactualWaitLowerBound is a lower bound on the same quantity,
	// derived from a mitigation's own failed retry attempts (if any exist —
	// e.g. probe's retries; noop and no mitigation never produce failures to
	// anchor this on).
	CounterfactualWaitLowerBound      time.Duration
	CounterfactualWaitLowerBoundKnown bool
}

const ztunnelHoldTimeout = 5 * time.Second

// PodResult is one pod's computed latency alongside its raw event.
type PodResult struct {
	PodEvent
	PatchLatency time.Duration
}

// Report aggregates PodResults from one burst run.
type Report struct {
	Pods             []PodResult
	Count            int
	TimeoutCount     int
	MeanPatchLatency time.Duration
	MaxPatchLatency  time.Duration
	P95PatchLatency  time.Duration

	// RoutingDelay* are aggregated only over pods with RoutingDelayKnown —
	// see PodEvent.RoutingDelay.
	RoutingDelayCount int
	MeanRoutingDelay  time.Duration
	MaxRoutingDelay   time.Duration

	// ConnectionFailed* are aggregated only over pods with
	// ConnectionFailedKnown — see PodEvent.ConnectionFailed.
	ConnectionFailedKnownCount int
	ConnectionFailedCount      int

	// Stats for cross check
	TimeoutVsFailureComparableCount int
	FailedAndTimedOut               int
	FailedNotTimedOut               int
	OKButTimedOut                   int
	OKNotTimedOut                   int

	// InitContainer* are aggregated only over pods with
	// InitContainerOutcomeKnown — see PodEvent.InitContainerOutcomeKnown.
	InitContainerKnownCount     int
	InitContainerReadyCount     int
	InitContainerExhaustedCount int
	MeanInitContainerElapsed    time.Duration
	MaxInitContainerElapsed     time.Duration

	// Mitigation* classifies pods whose real connection succeeded, using the
	// CounterfactualWait bounds above — a per-pod causal verdict, not a
	// population-level correlation:
	//   - MitigationDefinitelyUnnecessary: upper bound <= ztunnel's 5s hold —
	//     identity would have been ready in time even with zero mitigation.
	//   - MitigationDefinitelyNecessary: lower bound > 5s — proven necessary,
	//     since even the earliest possible true identity-ready moment
	//     exceeds the hold.
	//   - MitigationAmbiguous: neither bound resolves it — genuinely unknown
	//     without independent instrumentation of ztunnel's internal state.
	MitigationComparableCount       int
	MitigationDefinitelyUnnecessary int
	MitigationDefinitelyNecessary   int
	MitigationAmbiguous             int
}

// Compute derives per-pod latencies and aggregate statistics from a set of
// observed pod events.
func Compute(events []PodEvent) Report {
	r := Report{Pods: make([]PodResult, len(events))}
	if len(events) == 0 {
		return r
	}

	var total time.Duration
	latencies := make([]time.Duration, len(events))
	var routingTotal time.Duration
	var routingDelays []time.Duration
	var initElapsedTotal time.Duration
	var initElapsed []time.Duration
	for i, e := range events {
		latency := e.IPPatchedAtAPI.Sub(e.IPAssignedAt)
		r.Pods[i] = PodResult{PodEvent: e, PatchLatency: latency}
		latencies[i] = latency
		total += latency
		if e.ZtunnelTimeout {
			r.TimeoutCount++
		}
		if e.RoutingDelayKnown {
			routingTotal += e.RoutingDelay
			routingDelays = append(routingDelays, e.RoutingDelay)
		}
		if e.ConnectionFailedKnown {
			r.ConnectionFailedKnownCount++
			if e.ConnectionFailed {
				r.ConnectionFailedCount++
			}
		}
		if e.ConnectionFailedKnown {
			r.TimeoutVsFailureComparableCount++
			switch {
			case e.ConnectionFailed && e.ZtunnelTimeout:
				r.FailedAndTimedOut++
			case e.ConnectionFailed && !e.ZtunnelTimeout:
				r.FailedNotTimedOut++
			case !e.ConnectionFailed && e.ZtunnelTimeout:
				r.OKButTimedOut++
			default:
				r.OKNotTimedOut++
			}
		}
		if e.InitContainerOutcomeKnown {
			r.InitContainerKnownCount++
			if e.InitContainerReady {
				r.InitContainerReadyCount++
			} else {
				r.InitContainerExhaustedCount++
			}
			initElapsedTotal += e.InitContainerElapsed
			initElapsed = append(initElapsed, e.InitContainerElapsed)
		}
		if e.ConnectionFailedKnown && !e.ConnectionFailed && e.CounterfactualWaitUpperBoundKnown {
			r.MitigationComparableCount++
			switch {
			case e.CounterfactualWaitUpperBound <= ztunnelHoldTimeout:
				r.MitigationDefinitelyUnnecessary++
			case e.CounterfactualWaitLowerBoundKnown && e.CounterfactualWaitLowerBound > ztunnelHoldTimeout:
				r.MitigationDefinitelyNecessary++
			default:
				r.MitigationAmbiguous++
			}
		}
	}

	slices.Sort(latencies)
	if len(routingDelays) > 0 {
		slices.Sort(routingDelays)
		r.RoutingDelayCount = len(routingDelays)
		r.MeanRoutingDelay = routingTotal / time.Duration(len(routingDelays))
		r.MaxRoutingDelay = routingDelays[len(routingDelays)-1]
	}
	if len(initElapsed) > 0 {
		slices.Sort(initElapsed)
		r.MeanInitContainerElapsed = initElapsedTotal / time.Duration(len(initElapsed))
		r.MaxInitContainerElapsed = initElapsed[len(initElapsed)-1]
	}

	r.Count = len(events)
	r.MeanPatchLatency = total / time.Duration(len(events))
	r.MaxPatchLatency = latencies[len(latencies)-1]
	r.P95PatchLatency = latencies[p95Index(len(latencies))]
	return r
}

func p95Index(n int) int {
	idx := int(float64(n)*0.95 + 0.5)
	if idx >= n {
		idx = n - 1
	}
	return idx
}
