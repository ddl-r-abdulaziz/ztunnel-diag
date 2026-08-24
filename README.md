# ztunnel-diag

Reproduces and measures the ztunnel/kubelet startup race described in
[istio/istio#57674](https://github.com/istio/istio/issues/57674):

Under a startup burst (e.g. a node-scaling event bringing up many pods at
once), kubelet can take several seconds to patch a pod's IP onto its
API-server `status.podIP`. ztunnel acks a workload's first real connection
immediately and holds it open for a hardcoded 5s waiting for routing/identity
info to arrive before forwarding or dropping it — if that hasn't landed by
then, the connection dies with `timed out waiting for workload`. The 5s
itself is an unconditional `Duration::from_secs(5)` literal at the call site,
not a named constant or config knob — confirmed unchanged from 1.26.0 through
1.30.3: https://github.com/istio/ztunnel/blob/1.30.3/src/proxy.rs#L246

The test in this repository:
* 2 node cluster using minikube
   * One is warm, and hosts a service to try to connect to
   * The other is cordoned so that we get something close to a scale event  
* 100 workload pods, each with its own ServiceAccount all pinned to one node
  * cordon released, daemonset and pods arrive and spam the kubelet on the node
* Simultaneous load generating workers created by the test harness (host shares cpu
  with minikube)

Outcome: produces the measurable >5s delay causing ztunnel to drop some connections

This is the simplest repro of an issue we so most commonly with Karpenter's aggressive
node management.

## What this measures

`cmd/ztunnel-diag` creates the burst and, for each pod, records:

- **patch latency**: wall-clock time between the client issuing the pod's
  `Create` call and the API server first reflecting a non-empty
  `status.podIP` (observed via a `Watch`).
- **ztunnel timeout**: whether ztunnel's logs contain a
  `timed out waiting for workload '<pod>...' from xds` line for that pod
  (ztunnel identifies the workload by name in this message, not by IP).
- **routing delay** (requires `RUST_LOG=debug` on ztunnel — set by
  `hack/setup-minikube-ambient.sh`): how long ztunnel itself took, from first
  needing this workload's identity to actually opening its connection.
- **connection failed**: whether the workload pod's own `wget` against the
  target actually failed.

The summary line `ztunnel timeout matches connection failure: N/M pods` breaks
this into a 2x2 (failed+timeout, failed+no-timeout, ok+timeout, ok+no-timeout). 
A sizable off-diagonal count means something other than the 5s hold is
causing or masking failures.

This is an honest proxy, not an exact replay of the original investigation.
The original istio issue measured the gap between the *node-local* sandbox IP
allocation (from kubelet's own log) and the API patch — isolating just the
kubelet status-sync delay. This tool can only observe what the API server
sees, so "patch latency" here also includes scheduling and sandbox-creation
time. Routing delay is the tighter, more direct measurement; if you need the
node-local one too, correlate this tool's output with
`minikube ssh -- sudo journalctl -u kubelet` on the relevant node, the way
the original issue did.

## Running the test

```
make run
```

runs `make cluster` and `make echo-target` (both safe to re-run — they reuse
the existing cluster/deployment if already up) before invoking the test.

Subsequent runs with the same cluster can use:
```
go run ./cmd/ztunnel-diag
```

Defaults are the test as described above: 100 pods, pinned to
`--target-node ztunnel-diag-m02`, each hitting `--target
echo-target.ztunnel-diag.svc.cluster.local:8080`.

Flags exist for tweaking but are not needed to repro:

| flag | default | purpose |
|---|---|---|
| `--count` | 100 | pods created simultaneously, pinned to `--target-node` |
| `--target-node` | `ztunnel-diag-m02` | node being saturated |
| `--target` | `echo-target.<namespace>.svc.cluster.local:8080` | host:port every burst pod makes a real HTTP request against |
| `--window` | 60s | how long to wait for each pod's IP to appear before giving up on it |
| `--settle` | 30s | extra time after the window before scraping ztunnel's logs, so in-flight connections resolve first |
| `--json` | false | print the raw report as JSON |
| `--keep` | false | leave the burst pods running afterward instead of deleting them |
| `--host-load-workers` | `runtime.NumCPU()` | goroutines busy-looping on the host's real CPU during the burst+window+settle, to compete with minikube's Docker containers for actual scheduling time (0 disables) |
| `--init-container-mode` | `none` | `none` \| `noop` \| `probe` — see "Mitigation" below |


## Mitigation: `--init-container-mode probe`

An init container that retries a real HTTPS request to the k8s API server
(`$KUBERNETES_SERVICE_HOST`/`_PORT` — no DNS, no test-specific service, just
what constraint permits: the API server, control plane, node) until it gets
a genuine HTTP response line back, before the workload container starts. A
bare TCP-connect check doesn't work: ztunnel accepts a held connection's
handshake locally before it knows whether to forward or drop it, so `nc -z`
reports success instantly regardless of whether identity has landed. A raw
byte round trip doesn't work either — the API server's Go TLS stack silently
closes on malformed input instead of replying. Only an actual HTTP response
(`wget`'s `server returned error: HTTP/1.1 403 Forbidden` on an unauthorized
request) is unambiguous proof ztunnel genuinely forwarded the connection.

Modes (`--init-container-mode`): `none` (default), `noop` (bare `exit 0`, a
control for container-count overhead alone), `probe` (the mitigation above).

**A first pass at comparing these got the methodology wrong**: `none`, then
`noop`, then `probe` were each run once, hours apart. The repro's own
severity turned out to drift substantially over that time (`none` alone later
measured 97/100, then 26/100, then ~50/100 on unchanged code) — so that first
table (97/68/0) was three samples from a moving baseline, not a controlled
comparison. A user run of `noop` afterward showed 0/100, flatly contradicting
the 68/100 figure, which is what surfaced the problem.

The fix: interleave modes and run them back-to-back instead of in blocks, so
drift can't masquerade as a mode effect. Three cycles of `none → noop →
probe`, 100 pods each, run consecutively (2026-08-24, ~10:31–10:44):

| cycle | none   | noop  | probe |
|-------|--------|-------|-------|
| 1     | 50/100 | 0/100 | 0/100 |
| 2     | 56/100 | 0/100 | 0/100 |
| 3     | 49/100 | 0/100 | 0/100 |

Two things this settles, and one it doesn't:

- `none`'s baseline was stable within this tighter window (49-56/100) —
  today's ambient severity is real and much milder than the 97/100 seen
  earlier in the same day, confirming the repro's severity itself varies with
  host conditions, not just with mode.
- Both `noop` and `probe` reliably eliminated failures here (3/3 each,
  0/100) — under *this* baseline severity, a bare extra container is enough.
- **What this doesn't settle**: whether `noop` holds up under the more severe
  ~97/100 baseline seen earlier. The working hypothesis is that `noop`'s
  benefit comes from a small, fixed, incidental delay (extra container-start
  overhead), which is enough to cross a *marginal* race but not necessarily a
  *severely* skewed one (routing delays up to ~22s were observed at 97/100
  severity, vs. ~5s max at today's ~50/100 severity) — `probe`'s wait is
  adaptive and bounded by its own retry budget regardless of severity, so it
  shouldn't have this ceiling. This hasn't been verified by re-running the
  interleaved comparison under high-severity conditions; treat `probe` as the
  only one with a severity-independent guarantee until it is.

## What we explored and ruled out

Earlier iterations of this tool supported comparing mitigations
(`--init-container none|noop|sleep`, a `PILOT_PUSH_THROTTLE` toggle) and a
node-scale-up simulation (cordon a node, restart ztunnel there, uncordon
while pre-created pods bind). Removed for being exploration scaffolding
rather than the one reproduction this repo exists for — the findings are
worth keeping even though the code isn't:

- **initContainer mitigation.** A no-op initContainer workaround seen in
  production (a bare `sleep 1`) works by delaying the main container's
  start, giving kubelet's slow status-patch path more time to win the race —
  not by making any special network call. Whether a genuinely no-op
  initContainer (present, but no sleep) helps at all, purely from kubelet's
  own container-lifecycle overhead, was inconclusive at the pod counts tested
  in earlier iterations. Revisited with `--init-container-mode noop`: an
  interleaved comparison (see "Mitigation" above) eliminated failures
  entirely (0/100 x3) at a ~50/100 baseline severity, tying the deterministic
  `probe` mitigation — but that's under moderate severity only; it's unverified
  whether `noop` holds up at the ~97/100 severity seen elsewhere in this repo.
  `probe` doesn't share that open question, since its wait is adaptive rather
  than a fixed incidental delay.
- **`PILOT_PUSH_THROTTLE`.** Raising istiod's xDS push concurrency is a
  compounding factor, not the root cause — it can only help once istiod
  already knows about a burst of new identities and is limited in how fast
  it can push them all out. It doesn't address whatever makes istiod slow
  to learn about them in the first place.
- **Node-scale-up simulation.** A burst of `Create`s against an
  already-warm, steady-state node is a weaker simulation than a real
  karpenter/cluster-autoscaler scale-up, where kubelet is cold-starting *and*
  ztunnel/istio-cni-node are landing on that exact node for the first time at
  the same moment the pod burst binds. Reproducing that by cordoning a node,
  killing its ztunnel/istio-cni-node pods, and uncordoning while pods bind
  showed patch latency ~2.5x higher at the same pod count (40) than the same
  burst against a warm node (median 9.9s vs ~3.5-4s) — the compounding
  cold-start is real. But it still produced zero ztunnel timeouts even
  though most pods' patch latency exceeded 5s, and every connection was
  confirmed genuinely proxied (no bypass from ztunnel not being ready). The
  one real timeout only ever showed up from raw pod-count saturation — this
  is why the surviving test does that, on an already-healthy node, rather
  than chasing the cold-start angle further.
- **What actually gates the timeout.** Across all of this, istiod appeared
  able to push a workload's identity from information already available at
  pod creation (UID, ServiceAccount, namespace) independent of whether
  `status.podIP` had landed — elevated patch latency alone, even well past
  5s, did not reliably produce a timeout. The timeout showed up specifically
  under raw node-capacity saturation, i.e. istiod/ztunnel's *own* processing
  falling behind, not simply kubelet's status patch being late. What
  precisely pushes istiod/ztunnel into that state was never pinned down
  further than "enough concurrent distinct identities on one saturated
  node."
- **DNS absorbs and masks the real hold.** The workload's request originally
  went to the target by its k8s Service DNS name. That routes through
  ztunnel's own DNS proxy, which needs the *source* workload's identity to
  answer the query — a wait that hits the same 5s timeout as the real
  outbound connect (`timed out waiting for workload ... from xds`, logged
  from a `state:lookup{...}` span). But a client resolver (`musl`, on the
  busybox probe image) silently retries a timed-out DNS query on a fresh
  socket; by the time the retry lands, the identity has usually since
  arrived, DNS resolves, and the real outbound connect — the thing this tool
  actually means to test — never needs to wait at all. Net effect: 30/100
  pods logged a ztunnel timeout, `RoutingDelay` was known and under 11s for
  100/100, and **zero** pods' `wget` ever failed — a result that looked like
  either "no real repro" or "the timeout log is meaningless," when the actual
  cause was the probe never reaching the code path in question. Targeting the
  Service's ClusterIP directly (skip DNS, see `resolveTargetToClusterIP`)
  fixed it: 97/100 timeouts, 97/100 real `wget` failures, 100/100 agreement
  between the two.
- **Methodology bugs found along the way**, in case reproducing this
  elsewhere: a workload pod must make a real outbound connection that
  outlives ztunnel's 5s hold (an idle `sleep`, or a probe that closes itself
  early via stdin EOF, never exercises the race at all); every pod needs its
  own ServiceAccount (shared identity means only the first pod costs
  anything); ztunnel's timeout log matches by pod *name*, not IP;
  `client-go`'s default QPS/Burst throttles a concurrent burst client-side
  in a way that's easy to mistake for cluster latency; and a node's default
  pod-capacity ceiling (110 in minikube) silently caps any burst larger than
  that, leaving the excess permanently Pending and polluting the report as
  if they were just slow to patch.
