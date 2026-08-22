#!/usr/bin/env bash
# Spins up a local minikube cluster with vanilla upstream istio ambient
# profile (base + istiod + cni + ztunnel) so ztunnel-diag can reproduce the
# kubelet-status-patch / ztunnel-5s-timeout race documented in
# https://github.com/istio/istio/issues/57674.
#
# flags:
#   -K - keep the minikube cluster on exit (default: leave it running; pass
#        -K to also skip deleting it if it already exists)

set -euo pipefail

print_error() { echo -e "\n\033[30;41m ERROR \033[0m: $1" >&2; }
print_info()  { echo -e "\n\033[30;103m INFO \033[0m: $1\n"; }
print_success() { echo -e "\n\033[30;42m SUCCESS \033[0m: $1\n"; }

readonly profile_name="ztunnel-diag"
readonly test_namespace="ztunnel-diag"

keep_cluster=false
while getopts "K" opt; do
  case $opt in
    K) keep_cluster=true ;;
    \?) print_error "-$OPTARG"; exit 1 ;;
  esac
done

if ! command -v istioctl >/dev/null 2>&1; then
  print_error "istioctl not found. Install it: https://istio.io/latest/docs/setup/getting-started/#download"
  exit 1
fi

export MINIKUBE_HOME=${MINIKUBE_HOME:-$HOME/.minikube}
export KUBECONFIG=${KUBECONFIG:-$HOME/.kube/config}

if [[ "$keep_cluster" == "false" ]]; then
  print_info "Deleting any existing '$profile_name' minikube profile"
  minikube delete --profile "$profile_name" || true
fi

if ! minikube status --profile "$profile_name" >/dev/null 2>&1; then
  print_info "Starting minikube profile '$profile_name' (2 nodes)"
  # --nodes=2 at creation time, not a later `minikube node add`: on the
  # Docker driver, adding a node after the fact can land it on a different
  # Docker network than the control-plane node ("subnet is taken" during
  # add), leaving it with no route to the API server at all. Provisioning
  # both nodes together avoids that.
  # --cpus=2 deliberately starves istiod/ztunnel more than 4 would — they're
  # Docker containers sharing the host's real CPU, and less headroom for
  # their own processing under a burst makes the ztunnel timeout more likely
  # to actually cross the 5s budget rather than staying just under it.
  minikube start --profile "$profile_name" --nodes=2 --cpus=2 --memory=8g
else
  print_info "Reusing existing minikube profile '$profile_name'"
fi

readonly kubectl="minikube --profile $profile_name kubectl -- "

print_info "Installing istio ambient profile via istioctl"
istioctl install --set profile=ambient --skip-confirmation \
  --context "$profile_name"

print_info "Waiting for istiod, ztunnel and istio-cni-node to be ready"
$kubectl -n istio-system rollout status deployment/istiod --timeout=180s
$kubectl -n istio-system rollout status daemonset/ztunnel --timeout=180s
$kubectl -n istio-system rollout status daemonset/istio-cni-node --timeout=180s

print_info "Setting ztunnel to RUST_LOG=debug (needed for the routing-delay measurement)"
$kubectl -n istio-system set env daemonset/ztunnel RUST_LOG=debug
$kubectl -n istio-system rollout status daemonset/ztunnel --timeout=90s

print_info "Creating and labeling namespace '$test_namespace' for ambient dataplane mode"
$kubectl create namespace "$test_namespace" || true
$kubectl label namespace "$test_namespace" istio.io/dataplane-mode=ambient --overwrite

print_success "Cluster ready. Point ztunnel-diag at it with:
  KUBECONFIG=$KUBECONFIG go run ./cmd/ztunnel-diag --namespace $test_namespace"
