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
}

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
	}

	slices.Sort(latencies)
	if len(routingDelays) > 0 {
		slices.Sort(routingDelays)
		r.RoutingDelayCount = len(routingDelays)
		r.MeanRoutingDelay = routingTotal / time.Duration(len(routingDelays))
		r.MaxRoutingDelay = routingDelays[len(routingDelays)-1]
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
