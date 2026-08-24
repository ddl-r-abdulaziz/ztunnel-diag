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
| `--init-container-mode` | `none` | `none` \| `noop` \| `probe` \| `mixed` — see "Mitigation" below |


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
  ~97/100 baseline seen earlier. Attempted to re-verify under higher severity
  by raising `--host-load-workers` from the default (`NumCPU`) to 4x — didn't
  work: `none` measured 42/100, no higher than the moderate baseline. Past
  `NumCPU`, extra busy-loop goroutines don't add real host contention (Go's
  scheduler is already saturating every core; minikube's node containers are
  capped at 2 vCPUs regardless of host oversubscription). Reaching genuine
  high severity on demand would need less CPU allocated to the cluster itself
  (e.g. `--cpus=1` in `hack/setup-minikube-ambient.sh`), not more host load —
  untried so far.

### Recommendation: `probe`, not `noop` — and why `noop` was ruled out, not just under-tested

The obvious candidate for "simple and assured" was `noop`: no network calls,
so it adds nothing to an already-saturated control plane during a burst.
Putting it forward needed a real mechanistic reason it would hold up even
when identity takes a long time to arrive — not just "it scored 0/100 in
these runs" — because a fixed, incidental delay (kubelet's own
container-transition overhead, tens of seconds under this session's load —
see previous section) only protects up to whatever that delay happens to be
on a given cluster. The candidate hypothesis was that this wasn't fixed:
if the *same* node-level saturation that slows kubelet's container-lifecycle
machinery also slows ztunnel's own processing of an already-received xDS
push, `noop`'s "free" delay would scale up automatically in step with the
identity delay it needs to outlast, rather than being a ceiling that gets
outrun as severity increases.

Two things kill that hypothesis:

- **istiod isn't on the saturated node.** `kubectl get pods -n istio-system
  -l app=istiod -o wide` shows istiod on the control-plane node; the burst
  targets `ztunnel-diag-m02`. `--host-load-workers` CPU pressure on m02 never
  touches istiod's ability to *send* a push — at most it could slow
  ztunnel-on-m02's processing of a push already sent. There's no shared-CPU
  mechanism forcing the two delays to move together, only a partial,
  one-sided one.
- **Direct measurement confirms no coupling.** `--init-container-mode mixed`
  alternates `noop`/`probe` by pod index within one 100-pod burst, so both
  experience identical congestion at identical moments — `noop` pods give a
  clean transition-overhead measurement (trivial script, so the gap is pure
  kubelet), `probe` pods give ground-truth identity-wait (their own retry
  timing). Two runs, bucketed by creation order across the burst:

  | run | noop overhead (5 buckets) | probe identity-wait (5 buckets) | Pearson r |
  |-----|---------------------------|----------------------------------|-----------|
  | 1   | 10.8s 13.1s 9.9s 11.2s 11.5s | 8.7s 8.7s 8.9s 7.0s 9.2s | 0.01 |
  | 2   | 8.8s 10.6s 12.0s 11.1s 11.8s | 8.7s 8.1s 6.9s 8.3s 11.6s | 0.09 |

  Essentially zero correlation, twice. The individual worst cases confirm
  it's coincidence, not design: run 1's worst measured identity-wait (~17-18s,
  pod-97) came in *above* that run's worst `noop` overhead (~16.5s, pod-92) —
  `noop` would have been razor-thin or short had it drawn that slot. Run 2's
  worst identity-wait (~16s) came in *under* that run's worst `noop` overhead
  (~19.6s) — comfortable margin. Same cluster, same code, the ordering
  flips between runs. That's the signature of two independent processes
  landing in the same ballpark by chance, not one delay protecting against
  the other by mechanism. `noop`'s 0/100 record across this session is real,
  but it's a property of this specific 2-vCPU cluster's current dynamics, not
  a provable guarantee — and there's a realistic production shape (a fast,
  well-provisioned Karpenter node — kubelet transition ~200ms — joining a
  cluster where istiod is backed up from input volume, not host contention)
  where `noop`'s incidental delay would be far too small to matter at all.

**`probe`'s network load, the thing `noop` was chosen to avoid, turns out not
to be the cost it looks like.** A 100-pod burst generates ~200-300 HTTPS
requests to the API server spread over ~30s from `probe`'s retries — against
everything else already hitting that API server during a 100-pod burst
(kubelet, istio-cni, istiod), that's noise, not a second storm. The
interleaved data above backs this up directly: `probe`'s patch latency
(mean 25.6-29.0s across 3 cycles) is indistinguishable from `none`'s
(26.8-30.2s) — no visible degradation from `probe`'s own traffic. And unlike
`noop`, `probe` is self-terminating: it stops retrying the moment it gets a
real answer, rather than needing to be sized in advance for a worst case
that varies by cluster and severity.

This also settles the broader framing question of infra-level mitigation vs.
asking developers to harden their own startup code: `probe` already *is*
resilient startup logic (bounded retry against a real signal) — implemented
once, quietly, at the infra layer, instead of copy-pasted into every
workload's own init logic with whatever timeout each developer happens to
guess. That's a better outcome than asking teams to add 30+ second retries
to their own tools defensively (which risks masking unrelated startup bugs
in those tools) or trusting a few seconds of retry alone (measurably
insufficient against the delays observed here). It doesn't need `noop`'s
unproven "it happens to be enough" argument to make that case.

### Per-pod causal proof, not just aggregate failure rates

The interleaved table above answers "did mode X have fewer failures than mode
Y in this population of 100 pods" — a population-level correlation. It
doesn't say whether *any specific pod's* real connection would have failed
without the mitigation. Comparing different pods across different runs can't
answer that; a within-pod counterfactual can, using data every run already
produces:

- `IdentityReadyAt` (`ztlog.IdentityReadyAt`): the first connection ztunnel
  opened for this pod, to *any* destination — not scoped to the real target
  like `RoutingDelay` is. ztunnel's identity gate is per source workload, not
  per destination, so a mitigation init container's own probe connection
  succeeding is just as much proof identity had landed. This is always an
  upper bound on the true identity-ready moment (it can only be detected once
  something tries).
- The counterfactual anchor is when this pod's *first container* (init, if
  present, else `workload`) actually started running, from `pod.Status` — not
  ztunnel's netns-registration log line, which fires much earlier, during
  sandbox creation, before kubelet has necessarily gotten around to starting
  anything. Using netns-ready as the anchor was tried first and produces
  nearly identical (and equally large) numbers to the corrected anchor,
  which itself was a finding: kubelet/CRI's own backlog getting from
  sandbox-ready to actually starting a container — separate from anything
  ztunnel does — ran into the tens of seconds under this session's load, far
  larger than any init container's own self-reported execution time.
- If `IdentityReadyAt - anchor ≤ 5s`: **definitively unnecessary** — identity
  would've been ready in time even with zero mitigation delay, since even
  this upper-bound estimate clears the hold.
- If a mitigation retries (like `probe`), its failed attempts (`ztunnel`'s
  `connection failed` lines, via `ztlog.LastPreSuccessFailureAt`) give a
  **lower** bound on the true identity-ready moment too. If that lower bound
  already exceeds 5s: **definitively necessary**.
- Otherwise: **ambiguous** — resolving it needs an independent signal for
  when identity actually landed, not just what a connection attempt reveals
  after the fact. (Options considered but not built: polling ztunnel's own
  admin API for per-workload state from outside the pod, if such an endpoint
  exists in this version; a different, cleaner ztunnel debug-log event for
  xDS workload pushes. Kubernetes ephemeral containers were considered and
  rejected for this — they don't participate in init-container sequencing,
  which is the right property, but their injection path
  [API PATCH → kubelet reconcile → CRI start] can itself take multiple
  seconds under the same saturation being measured, making them unreliable
  for exactly the fast-resolving cases this gap needs.)

Validated against real runs: `probe` (which always has retry evidence)
resolved **85/100 pods definitively** — 33 provably unnecessary, 52 provably
necessary, 15 ambiguous. `noop` (no retries, hence no lower bound ever)
resolved **0/100** — always ambiguous, exactly as expected. Closing this gap
per-pod for `noop` would still need the independent-instrumentation options
above — but it turned out not to matter for choosing a mitigation:
`--init-container-mode mixed` (see "Recommendation" above) answered the
question this gap was blocking — whether `noop`'s delay is mechanistically
tied to the real identity delay — directly, without per-pod resolution,
by correlating the two across a shared burst instead.

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
  `probe` mitigation at that severity — but `--init-container-mode mixed`
  (see "Recommendation" above) later showed this was coincidental overlap,
  not a mechanism: `noop`'s incidental delay and the real identity delay it
  needs to outlast don't correlate (Pearson r ≈ 0.01, 0.09 across two runs),
  and istiod runs on the control-plane node, untouched by the burst node's
  saturation, so there's no shared-bottleneck reason to expect them to.
  `probe` was chosen as the recommended mitigation instead.
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
