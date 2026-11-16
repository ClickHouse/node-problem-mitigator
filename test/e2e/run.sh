#!/usr/bin/env bash
set -Eeuo pipefail

KIND=${KIND:-kind}
KUBECTL=${KUBECTL:-kubectl}
HELM=${HELM:-helm}
KO=${KO:-ko}
KIND_CLUSTER=${KIND_CLUSTER:-node-problem-mitigator-test-e2e}
KIND_NODE_IMAGE=${KIND_NODE_IMAGE:-kindest/node:v1.34.8@sha256:02722c2dedddcfc00febf5d27fbeb9b7b2c14294c82109ff4a85d89ac9ba3256}
KWOK_VERSION=${KWOK_VERSION:-v0.8.0}
E2E_NAMESPACE=node-problem-mitigator-e2e

for tool in "$KIND" "$KUBECTL" "$HELM" "$KO" go; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "Required executable not found: $tool" >&2
    exit 1
  }
done

TMP_DIR=$(mktemp -d)
KUBECONFIG="$TMP_DIR/kubeconfig"
ARCHIVE="$TMP_DIR/node-problem-mitigator.tar"
export KUBECONFIG
CREATED_CLUSTER=false
CLUSTER_READY=false

cluster_exists() {
  "$KIND" get clusters | awk -v name="$KIND_CLUSTER" '$0 == name { found = 1 } END { exit !found }'
}

print_diagnostics() {
  [[ "$CLUSTER_READY" == true ]] || return 0
  echo "=== Kubernetes resources ===" >&2
  "$KUBECTL" get nodes -o wide >&2 || true
  "$KUBECTL" get pods -A -o wide >&2 || true
  "$KUBECTL" get jobs -n "$E2E_NAMESPACE" -o wide >&2 || true
  "$KUBECTL" describe pods -n "$E2E_NAMESPACE" >&2 || true
  echo "=== Helm releases ===" >&2
  "$HELM" list --all-namespaces >&2 || true
  echo "=== Controller and KWOK logs ===" >&2
  "$KUBECTL" logs -n "$E2E_NAMESPACE" deployment/node-problem-mitigator --all-containers=true --tail=200 >&2 || true
  "$KUBECTL" logs -n kube-system deployment/kwok-controller --all-containers=true --tail=200 >&2 || true
  jobs=$("$KUBECTL" get jobs -n "$E2E_NAMESPACE" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null) || jobs=""
  for job in $jobs; do
    "$KUBECTL" logs -n "$E2E_NAMESPACE" "job/$job" --all-containers=true --tail=100 >&2 || true
  done
  echo "=== Recent events ===" >&2
  "$KUBECTL" get events -A --sort-by=.lastTimestamp >&2 || true
}

cleanup() {
  status=$?
  trap - EXIT
  if (( status != 0 )); then
    print_diagnostics
  fi
  rm -f "$ARCHIVE" "$KUBECONFIG"
  rmdir "$TMP_DIR" 2>/dev/null || true
  if [[ "$CREATED_CLUSTER" == true && "${KEEP_E2E_CLUSTER:-false}" != true ]]; then
    "$KIND" delete cluster --name "$KIND_CLUSTER" || {
      echo "Failed to delete kind cluster $KIND_CLUSTER" >&2
      (( status != 0 )) || status=1
    }
  fi
  exit "$status"
}
trap cleanup EXIT

if cluster_exists; then
  echo "Reusing kind cluster '$KIND_CLUSTER'."
else
  echo "Creating kind cluster '$KIND_CLUSTER' with image '$KIND_NODE_IMAGE'."
  CREATED_CLUSTER=true
  "$KIND" create cluster --name "$KIND_CLUSTER" --image "$KIND_NODE_IMAGE" --kubeconfig "$KUBECONFIG"
fi
if [[ "$CREATED_CLUSTER" != true ]]; then
  "$KIND" export kubeconfig --name "$KIND_CLUSTER" --kubeconfig "$KUBECONFIG"
fi
export KUBECONTEXT="kind-$KIND_CLUSTER"
"$KUBECTL" config use-context "$KUBECONTEXT"
"$KUBECTL" cluster-info
CLUSTER_READY=true

KWOK_RELEASE="https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VERSION}"
"$KUBECTL" apply -f "${KWOK_RELEASE}/kwok.yaml"
"$KUBECTL" apply -f "${KWOK_RELEASE}/stage-fast.yaml"
"$KUBECTL" rollout status -n kube-system deployment/kwok-controller --timeout=180s

platform="linux/$(go env GOARCH)"
echo "Building controller image for ${platform}."
KO_DOCKER_REPO=example.com/node-problem-mitigator IMAGE_TAG=e2e \
  "$KO" build --bare --push=false --tarball "$ARCHIVE" \
    --platform="$platform" --tags e2e ./cmd/
"$KIND" load image-archive "$ARCHIVE" --name "$KIND_CLUSTER"

"$HELM" uninstall node-problem-mitigator --namespace "$E2E_NAMESPACE" \
  --ignore-not-found --wait
"$HELM" install node-problem-mitigator helm/node-problem-mitigator \
  --namespace "$E2E_NAMESPACE" --create-namespace \
  --values test/e2e/values.yaml \
  --set-string image.repository=example.com/node-problem-mitigator \
  --set-string image.tag=e2e --set image.pullPolicy=Never \
  --wait --timeout=180s
"$KUBECTL" rollout status -n "$E2E_NAMESPACE" \
  deployment/node-problem-mitigator --timeout=180s

echo "Running tagged e2e suite."
go test -count=1 -tags=e2e ./test/e2e -v -ginkgo.v -timeout 15m
