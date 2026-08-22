.DEFAULT_GOAL := help

KUBECONFIG ?= $(HOME)/.kube/config
export KUBECONFIG

# .bin/ (populated by `make deps`) takes priority over whatever minikube,
# kubectl, or istioctl happen to be on the developer's own PATH — that's how
# we ended up silently installing an EOL istio 1.26.0 for a while, from a
# stale system istioctl.
BIN_DIR := $(CURDIR)/.bin
export PATH := $(BIN_DIR):$(PATH)

# CREATE=false reuses an existing cluster instead of tearing it down and
# rebuilding it (e.g. `make run CREATE=false`).
CREATE ?= true

.PHONY: help test build serve deps cluster destroy-cluster echo-target run

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

test: ## Run all unit tests
	go test ./...

deps: ## Download pinned minikube/kubectl/istioctl into .bin/ (idempotent)
	hack/install-deps.sh

cluster: deps ## Start (or reuse) the ztunnel-diag minikube ambient cluster. Pass CREATE=false to reuse an existing cluster instead of recreating it.
	@if [ "$(CREATE)" = "false" ]; then \
		hack/setup-minikube-ambient.sh -K; \
	else \
		hack/setup-minikube-ambient.sh; \
	fi

destroy-cluster: deps ## Tear down the ztunnel-diag minikube profile entirely
	minikube delete --profile ztunnel-diag

echo-target: cluster ## Deploy the echo-target burst destination
	hack/deploy-echo-target.sh

run: cluster echo-target ## Ensure the cluster is up and run the ztunnel-diag test
	go run ./cmd/ztunnel-diag

