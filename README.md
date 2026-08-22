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

**The one test:** 100 workload pods, each with its own ServiceAccount (istio
identity is per-namespace-and-ServiceAccount, not per-pod, so this matters —
see below), all pinned to one node — saturating its ~110 pod capacity — each
making a real HTTP request against a dedicated service pinned to the *other*
node, so the target itself stays warm and unaffected by the saturation. Run
it enough times at this scale and a real `timed out waiting for workload`
shows up, with the offending pod's measured routing delay landing right at
or past the 5s budget while every other pod stays under it.

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
  needing this workload's identity to actually opening its connection. This
  is the number that actually matters against the 5s budget — **not** the
  access log's `connection complete ... duration=` field, which is the whole
  connection's lifetime (dominated by however long the client holds the
  socket open) and is easy to mistake for ztunnel's internal wait. One
  nuance: the probe connects to a hostname (the target service's DNS name),
  so ztunnel's DNS proxy resolves it first — that DNS query is itself an
  identity-consuming event and is usually what "first needs the identity," a
  moment before the TCP connect that follows. Same underlying identity-gate
  subsystem either way, but the measured delay reflects "first touched the
  gate for any reason," not specifically "time to open the TCP connection."

This is an honest proxy, not an exact replay of the original investigation.
The original istio issue measured the gap between the *node-local* sandbox IP
allocation (from kubelet's own log) and the API patch — isolating just the
kubelet status-sync delay. This tool can only observe what the API server
sees, so "patch latency" here also includes scheduling and sandbox-creation
time. Routing delay is the tighter, more direct measurement; if you need the
node-local one too, correlate this tool's output with
`minikube ssh -- sudo journalctl -u kubelet` on the relevant node, the way
the original issue did.

## Setup

```
make cluster
make echo-target
```

(`make cluster` runs `hack/setup-minikube-ambient.sh`, `make echo-target` runs
`hack/deploy-echo-target.sh`.) `make destroy-cluster` tears the profile down
entirely when you're done.

The first starts (or reuses) a 2-node minikube profile named `ztunnel-diag`
(`--cpus=2` per node — deliberately tight), installs upstream istio's ambient
profile via `istioctl`, sets `RUST_LOG=debug` on ztunnel (needed for routing
delay), and labels a `ztunnel-diag` namespace for the ambient dataplane.
Requires `istioctl` on PATH. Pass `-K` to skip deleting a pre-existing
cluster with the same profile name (or `make cluster CREATE=false`).

The second deploys the burst's target: a small `busybox httpd` pinned to the
control-plane node (`ztunnel-diag`), fronted by a ClusterIP `Service` at
`echo-target.ztunnel-diag.svc.cluster.local:8080`.

Making the race actually cross the 5s budget needs istiod/ztunnel's own
processing to fall behind — elevated patch latency alone doesn't reliably do
it (istiod can push a workload's identity from data already available at pod
creation, independent of `status.podIP` — see "What we explored and ruled
out"). An in-cluster CPU-hog pod pinned to istiod's node was tried first and
didn't move the needle: it only competes within the cluster's own CPU
accounting. `cmd/ztunnel-diag` instead busy-loops goroutines directly on the
*host* machine for the duration of the burst (`--host-load-workers`, default
`runtime.NumCPU()`) — minikube's nodes are just Docker containers sharing
the host's real CPU, so this competes with istiod/ztunnel at the actual OS
scheduling level.

## Running the test

```
make run
```

runs `make cluster` and `make echo-target` (both safe to re-run — they reuse
the existing cluster/deployment if already up) before invoking the test.
Once those are up, you can also invoke the test directly:

```
go run ./cmd/ztunnel-diag
```

Defaults are the test as described above: 100 pods, pinned to
`--target-node ztunnel-diag-m02`, each hitting `--target
echo-target.ztunnel-diag.svc.cluster.local:8080`. Flags exist to adjust
these, but the point of this repo is this one test, not a knob-exploration
harness — see "What we explored and ruled out" below for the paths that
turned out not to matter.

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

Expected result, per the last confirmed run (95 pods, since bumped to the
100 default): every pod but one stayed under the 5s routing-delay budget;
the one pod that timed out had the single highest routing delay of the
batch (5.789s), and the next-highest (4.891s) didn't time out — a clean
line right at the 5s cutoff.

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
  own container-lifecycle overhead (image pull, CRI create/start, PLEG
  relist), was inconclusive at the pod counts tested — plausible but not
  confirmed either way.
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

## Development

Pure logic (`internal/report`, `internal/ztlog`) is unit tested and has no
cluster dependency:

```
go test ./...
go vet ./...
```

`cmd/ztunnel-diag` itself is an integration harness against a real cluster
and isn't covered by the unit tests above.
