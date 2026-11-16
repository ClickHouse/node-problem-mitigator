# Development

You can build the controller and render the chart without a cluster.
Use the test suites below to check controller behavior and a full Helm deployment.

[Back to README](../README.md)

## Local build

```sh
make build
./bin/node-problem-mitigator --help
helm template node-problem-mitigator ./helm/node-problem-mitigator
```

Build and run a local container without publishing it:

```sh
make docker-build IMG=node-problem-mitigator IMAGE_TAG=dev
docker run --rm --network none node-problem-mitigator:dev --help
```

The local target builds for `linux/$(go env GOARCH)`. Set `LOCAL_PLATFORM` to override it.
`make docker-push` publishes images for both Linux architectures.

## Dry-run against a test cluster

This command uses your current kubeconfig and permissions, rather than the chart's ServiceAccount.

```sh
go run ./cmd --namespace=node-problem-mitigator --dry-run --leader-elect
```

Create an enabled [ConfigMap](configuration.md) with watched conditions in that namespace before running the controller.

| Flag | Default | Purpose |
|---|---|---|
| `--namespace` | Required | Namespace for the controller's ConfigMaps. |
| `--dry-run` | `false` | Logs decisions without changing nodes or launching Jobs. |
| `--leader-elect` | `false` | Enables leader election. The Helm chart enables it by default. |
| `--metrics-bind-address` | `0` | Metrics address; `0` disables it. `:8080` serves HTTP. |
| `--health-probe-bind-address` | `:8081` | Health probe address. |
| `--version` | `false` | Prints the version and exits. |
| `--debug` | `false` | Logs at debug level instead of info. |
| `--log-format` | `console` | Log format: `console` or `json`. |

## Tests

Run the unit and envtest suite:

```sh
make test
```

Install kind, kubectl, Helm, ko, and a Docker- or Podman-compatible container runtime, then run the end-to-end tests:

```sh
make test-e2e
```

The [end-to-end suite](../test/e2e/) builds your controller image and installs it in kind through the Helm chart.
KWOK simulates worker nodes and workloads. The controller and action Jobs run on real kind nodes.
The suite checks condition-driven tainting, global cooldown, Job ordering, and PodDisruptionBudget enforcement.

The runner deletes clusters it creates, even if a test fails. It reuses existing named kind clusters and leaves them in place.
Run `KEEP_E2E_CLUSTER=true make test-e2e` to keep a new cluster for debugging.
When a test fails, the output includes controller, KWOK, and Job logs, plus resource and Event details.

## Releases

Push a [SemVer](https://semver.org/) tag, such as `v1.0.0` or `v1.1.0-rc.1`, to run CI and publish binaries, an image, and a Helm chart:

- Image repository: `ghcr.io/clickhouse/node-problem-mitigator`
- Chart: `oci://ghcr.io/clickhouse/charts/node-problem-mitigator`

Image tags and chart versions use the release version without the leading `v`.

The [release workflow](../.github/workflows/release.yaml) generates build provenance attestations.

Authenticate with GHCR, then verify the image from the latest stable release:

```sh
gh auth token | docker login ghcr.io --username "$(gh api user --jq .login)" --password-stdin &&
image_tag=$(gh api repos/ClickHouse/node-problem-mitigator/releases/latest \
  --jq '.tag_name | ltrimstr("v")') &&
gh attestation verify \
  "oci://ghcr.io/clickhouse/node-problem-mitigator:${image_tag}" \
  --repo ClickHouse/node-problem-mitigator
```
