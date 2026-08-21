#!/usr/bin/env bash
# Deploys a small, dedicated HTTP target every burst pod connects to, pinned
# to the control-plane node — the *other* node from the one ztunnel-diag
# saturates with the burst (--target-node, default ztunnel-diag-m02) — so it
# stays warm and steady-state across the run. Burst pods should exercise
# ztunnel's own identity-routing path, not add load to (or depend on) the
# k8s API server as a side effect of being the only thing around to connect
# to.
#
# Usage: hack/deploy-echo-target.sh

set -euo pipefail

readonly profile="ztunnel-diag"
readonly namespace="ztunnel-diag"
readonly warm_node="ztunnel-diag"
readonly kubectl="minikube --profile $profile kubectl -- "

$kubectl apply -n "$namespace" -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: echo-target
  labels:
    app: echo-target
spec:
  replicas: 1
  selector:
    matchLabels:
      app: echo-target
  template:
    metadata:
      labels:
        app: echo-target
    spec:
      nodeSelector:
        kubernetes.io/hostname: $warm_node
      containers:
        - name: echo-target
          image: busybox:1.36
          command: ["httpd", "-f", "-p", "8080", "-h", "/tmp"]
          ports:
            - containerPort: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: echo-target
spec:
  selector:
    app: echo-target
  ports:
    - port: 8080
      targetPort: 8080
EOF

$kubectl -n "$namespace" rollout status deployment/echo-target --timeout=90s

echo "echo-target ready at echo-target.$namespace.svc.cluster.local:8080 (pinned to node $warm_node)"
