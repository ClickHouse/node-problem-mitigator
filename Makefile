# Container repository and tag for ko builds.
IMG ?= node-problem-mitigator
IMAGE_TAG ?= dev

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# Prints every target that has a '## description', grouped under its '##@ category'.

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: fmt vet setup-envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test $$(go list ./... | grep -v /e2e) -v -coverprofile cover.out

KIND_CLUSTER ?= node-problem-mitigator-test-e2e

KIND_NODE_IMAGE ?= kindest/node:v1.34.8@sha256:02722c2dedddcfc00febf5d27fbeb9b7b2c14294c82109ff4a85d89ac9ba3256

.PHONY: test-e2e
test-e2e: ## Run the end-to-end tests against a Helm-installed controller in Kind.
	KIND="$(KIND)" KUBECTL="$(KUBECTL)" HELM="$(HELM)" KO="$(KO)" \
		KIND_CLUSTER="$(KIND_CLUSTER)" KIND_NODE_IMAGE="$(KIND_NODE_IMAGE)" \
		bash test/e2e/run.sh

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	"$(GOLANGCI_LINT)" run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	"$(GOLANGCI_LINT)" config verify

.PHONY: vuln
vuln: govulncheck ## Report known vulnerabilities that the code reaches.
	"$(GOVULNCHECK)" ./...

.PHONY: diagrams
# The sed keeps <img> embeds at native size but fits the SVG to the window
# width when the file is opened directly (raw link).
diagrams: ## Regenerate docs/*.svg from the d2 sources (needs d2 >= 0.8).
	for src in docs/*.d2; do \
		d2 --layout elk --scale 1 \
			--elk-nodeNodeBetweenLayers 45 \
			--elk-padding "[top=30,left=30,bottom=30,right=30]" \
			"$$src" "$${src%.d2}.svg" || exit 1; \
		out="$${src%.d2}.svg"; \
		sed -E '1s|(<svg[^>]*) width="([0-9]+)" height="([0-9]+)"|\1 width="\2" height="\3" style="width:100vw;height:calc(100vw * \3 / \2)"|' "$$out" > "$$out.tmp" \
			&& mv "$$out.tmp" "$$out" || exit 1; \
	done

##@ Build

.PHONY: build
build: fmt vet ## Build the controller binary.
	go build -o bin/node-problem-mitigator ./cmd

.PHONY: run
run: fmt vet ## Run the controller from your host.
	go run ./cmd


PLATFORMS ?= linux/arm64,linux/amd64
LOCAL_PLATFORM ?= linux/$(shell go env GOARCH)

.PHONY: docker-build
docker-build: ## Build a local single-platform image without pushing.
	KO_DOCKER_REPO=$(IMG) IMAGE_TAG=$(IMAGE_TAG) ko build --bare --local \
		--platform=$(LOCAL_PLATFORM) \
		--tags $(IMAGE_TAG) ./cmd/

.PHONY: docker-push
docker-push: ## Build and push container image with ko.
	KO_DOCKER_REPO=$(IMG) IMAGE_TAG=$(IMAGE_TAG) ko build --bare \
		--platform=$(PLATFORMS) \
		--tags $(IMAGE_TAG) ./cmd/

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
HELM ?= helm
KO ?= ko
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint
GOVULNCHECK = $(LOCALBIN)/govulncheck

## Tool Versions
#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell v='$(call gomodver,sigs.k8s.io/controller-runtime)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_VERSION manually (controller-runtime replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?([0-9]+)\.([0-9]+).*/release-\1.\2/')

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= 1.33

GOLANGCI_LINT_VERSION ?= v2.14.0
GOVULNCHECK_VERSION ?= v1.9.0

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

.PHONY: govulncheck
govulncheck: $(GOVULNCHECK) ## Download govulncheck locally if necessary.
$(GOVULNCHECK): $(LOCALBIN)
	$(call go-install-tool,$(GOVULNCHECK),golang.org/x/vuln/cmd/govulncheck,$(GOVULNCHECK_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef
