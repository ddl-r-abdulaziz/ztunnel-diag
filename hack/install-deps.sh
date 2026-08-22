#!/usr/bin/env bash
# Downloads pinned minikube, kubectl, and istioctl into .bin/, so every
# other target in this repo uses known-good versions instead of whatever
# happens to be on the developer's PATH. That's exactly how we ended up
# silently installing an EOL istio 1.26.0 for a while — the system istioctl
# just happened to be that version.
#
# Idempotent: skips a tool whose .bin/ copy already reports the pinned
# version.
#
# Usage: hack/install-deps.sh

set -euo pipefail

readonly MINIKUBE_VERSION="v1.38.1"
readonly KUBECTL_VERSION="v1.33.13"
readonly ISTIOCTL_VERSION="1.30.3"

print_info() { echo -e "\n\033[30;103m INFO \033[0m: $1\n"; }

root_dir="$(cd "$(dirname "$0")/.." && pwd)"
bin_dir="$root_dir/.bin"
mkdir -p "$bin_dir"

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$arch" in
  x86_64) arch="amd64" ;;
  aarch64) arch="arm64" ;;
esac

case "$os" in
  darwin) istio_os="osx" ;;
  linux) istio_os="linux" ;;
  *) echo "unsupported OS: $os" >&2; exit 1 ;;
esac

already_have() {
  local bin="$1" version_args="$2" want="$3"
  [[ -x "$bin" ]] || return 1
  # Capture into a variable rather than piping to `grep -q`: grep exits on
  # its first match without reading the rest of stdin, which can SIGPIPE a
  # still-writing multi-line version command — under `pipefail` that fails
  # this check even on a real match (bit us on minikube's two-line output).
  local output
  # shellcheck disable=SC2086
  output="$("$bin" $version_args 2>/dev/null || true)"
  [[ "$output" == *"$want"* ]]
}

if already_have "$bin_dir/minikube" "version" "$MINIKUBE_VERSION"; then
  print_info "minikube $MINIKUBE_VERSION already in .bin/"
else
  print_info "Installing minikube $MINIKUBE_VERSION ($os/$arch)"
  curl -sL "https://storage.googleapis.com/minikube/releases/${MINIKUBE_VERSION}/minikube-${os}-${arch}" \
    -o "$bin_dir/minikube"
  chmod +x "$bin_dir/minikube"
fi

if already_have "$bin_dir/kubectl" "version --client" "$KUBECTL_VERSION"; then
  print_info "kubectl $KUBECTL_VERSION already in .bin/"
else
  print_info "Installing kubectl $KUBECTL_VERSION ($os/$arch)"
  curl -sL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/${os}/${arch}/kubectl" \
    -o "$bin_dir/kubectl"
  chmod +x "$bin_dir/kubectl"
fi

if already_have "$bin_dir/istioctl" "version --remote=false" "$ISTIOCTL_VERSION"; then
  print_info "istioctl $ISTIOCTL_VERSION already in .bin/"
else
  print_info "Installing istioctl $ISTIOCTL_VERSION ($istio_os/$arch)"
  tmp_tar="$(mktemp)"
  curl -sL "https://github.com/istio/istio/releases/download/${ISTIOCTL_VERSION}/istioctl-${ISTIOCTL_VERSION}-${istio_os}-${arch}.tar.gz" \
    -o "$tmp_tar"
  tar -xzf "$tmp_tar" -C "$bin_dir" istioctl
  rm -f "$tmp_tar"
  chmod +x "$bin_dir/istioctl"
fi

print_info "Pinned tool versions in $bin_dir:"
"$bin_dir/minikube" version
"$bin_dir/kubectl" version --client
"$bin_dir/istioctl" version --remote=false
