.DEFAULT_GOAL := help

KUBECONFIG ?= $(HOME)/.kube/config
export KUBECONFIG

# CREATE=false reuses an existing cluster instead of tearing it down and
# rebuilding it (e.g. `make run CREATE=false`).
CREATE ?= true

.PHONY: help test build serve cluster destroy-cluster echo-target run

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

test: ## Run all unit tests
	go test ./...

cluster: ## Start (or reuse) the ztunnel-diag minikube ambient cluster. Pass CREATE=false to reuse an existing cluster instead of recreating it.
	@if [ "$(CREATE)" = "false" ]; then \
		hack/setup-minikube-ambient.sh -K; \
	else \
		hack/setup-minikube-ambient.sh; \
	fi

destroy-cluster: ## Tear down the ztunnel-diag minikube profile entirely
	minikube delete --profile ztunnel-diag

echo-target: cluster ## Deploy the echo-target burst destination
	hack/deploy-echo-target.sh

run: cluster echo-target ## Ensure the cluster is up and run the ztunnel-diag test
	go run ./cmd/ztunnel-diag

