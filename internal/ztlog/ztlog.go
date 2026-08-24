// Package ztlog scans ztunnel log lines for the workload-identity timeout
// that shows up when kubelet's status.podIP patch (or ztunnel's own xDS wait)
// loses the race against ztunnel's hardcoded 5s hold.
package ztlog

import (
	"strings"
	"time"
)

const timeoutPhrase = "timed out waiting for workload"

// MatchesTimeoutForPod reports whether a ztunnel log line records a workload
// timeout for the given pod name. ztunnel identifies the workload by name in
// this message, e.g.:
//
//	timed out waiting for workload 'my-pod.my-namespace (my-pod)' from xds
//
// not by IP, so matching looks for the pod name inside the quoted identity
// (`'<pod>.`) or the parenthesized short form (`(<pod>)`) rather than a bare
// substring — a bare substring would false-positive on one pod's name being
// a prefix of another's. An empty pod name never matches.
func MatchesTimeoutForPod(line, pod string) bool {
	if pod == "" {
		return false
	}
	if !strings.Contains(line, timeoutPhrase) {
		return false
	}
	return strings.Contains(line, "'"+pod+".") || strings.Contains(line, "("+pod+")")
}

// RoutingDelay returns how long it took, for the given pod, from first
// needing that workload's identity to its connection to targetService
// actually opening.
//
// targetService scopes the end marker to that specific destination
// (ztunnel's dst.service field) rather than any "connection opened" line for
// this pod — a mitigation init container's own probe connection shares the
// workload's identity from ztunnel's point of view, so dst.service is the
// only thing that tells the two apart in the log.
//
// It requires RUST_LOG=debug on ztunnel (the default "info" level doesn't
// emit these lines).
func RoutingDelay(logs []string, pod, namespace, targetService string) (time.Duration, bool) {
	waitMarker := "wl=" + pod + "." + namespace
	openMarker := `src.workload="` + pod + `"`
	targetMarker := `dst.service="` + targetService + `"`

	var start, end time.Time
	for _, line := range logs {
		if start.IsZero() && strings.Contains(line, "wait for workload") && strings.Contains(line, waitMarker) {
			if ts, ok := parseTimestamp(line); ok {
				start = ts
			}
			continue
		}
		if !start.IsZero() && end.IsZero() && strings.Contains(line, "connection opened") &&
			strings.Contains(line, openMarker) && strings.Contains(line, targetMarker) {
			if ts, ok := parseTimestamp(line); ok {
				end = ts
			}
			break
		}
	}
	if start.IsZero() || end.IsZero() {
		return 0, false
	}
	return end.Sub(start), true
}

// workloadIdentityMarker is the exact substring ztunnel embeds in prose
// error messages to identify a workload — `<pod>.<namespace> (<pod>)`.
func workloadIdentityMarker(pod, namespace string) string {
	return pod + "." + namespace + " (" + pod + ")"
}

// IdentityReadyAt returns the timestamp of the first connection ztunnel
// opened for this pod, to any destination — unlike RoutingDelay, this is
// deliberately NOT scoped to a specific target. ztunnel's identity gate is
// per source workload, not per destination, so a mitigation init container's
// own probe connection succeeding is just as much proof identity had landed
// as the real workload's connection succeeding. This is always an upper
// bound on the true identity-ready moment: it can only be detected once
// something actually tries, and identity may have landed earlier without any
// attempt happening to reveal it.
func IdentityReadyAt(logs []string, pod, namespace string) (time.Time, bool) {
	openMarker := `src.workload="` + pod + `"`
	for _, line := range logs {
		if strings.Contains(line, "connection opened") && strings.Contains(line, openMarker) {
			return parseTimestamp(line)
		}
	}
	return time.Time{}, false
}

// LastPreSuccessFailureAt returns the timestamp of the last "connection
// failed" line for this pod. Intended for pods whose real connection
// eventually succeeded (RoutingDelay/IdentityReadyAt known).
func LastPreSuccessFailureAt(logs []string, pod, namespace string) (time.Time, bool) {
	marker := workloadIdentityMarker(pod, namespace)
	var last time.Time
	found := false
	for _, line := range logs {
		if !strings.Contains(line, "connection failed") || !strings.Contains(line, marker) {
			continue
		}
		if ts, ok := parseTimestamp(line); ok {
			last = ts
			found = true
		}
	}
	return last, found
}

func parseTimestamp(line string) (time.Time, bool) {
	field, _, found := strings.Cut(line, "\t")
	if !found {
		return time.Time{}, false
	}
	ts, err := time.Parse(time.RFC3339Nano, field)
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}
