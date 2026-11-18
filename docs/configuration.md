# Configuration

Configure the controller through Helm values. Helm writes them to the
`node-problem-mitigator-config` ConfigMap in the controller's namespace.
The controller reads this ConfigMap each time it checks a node.

[Back to README](../README.md)

## Helm values

See the [chart defaults](../helm/node-problem-mitigator/values.yaml) for all values.

| Value | Default | Purpose |
|---|---|---|
| `app.enabled` | `false` | Enables mitigation. |
| `app.dryRun` | `false` | Logs decisions without changing nodes or launching Jobs. |
| `app.leaderElect` | `true` | Keeps one controller active. |
| `app.config` | Defined below | Conditions, cooldown, and exclusions. |
| `jobs` | `{}` | Map of named Job templates; a `null` Job entry is ignored. |
| `serviceAccount` | Creates an account named after the release | Controller identity. |
| `serviceMonitor.enabled` | `false` | Enables Prometheus scraping through a ServiceMonitor. |
| `prometheusRule.enabled` | `false` | Enables the [built-in alerts](operations.md#alerts). |
| `image.repository` | `ghcr.io/clickhouse/node-problem-mitigator` | Controller image. |
| `image.tag` | Chart `appVersion` | Controller version. |

`replicas`, `resources`, `podLabels`, `podAnnotations`, `nodeSelector`, `tolerations`,
`affinity`, `priorityClassName`, `podSecurityContext`, and the `extra*` values
configure the Deployment. The chart renders `app.config`, Job templates, and
Prometheus rule label values with Helm `tpl`.

## Conditions and exclusions

```yaml
app:
  enabled: true
  dryRun: true
  config:
    conditions:
      KernelDeadlock:
        drain: true # optional default
        taint: true # optional default
        mitigationJob:
          name: terminate
      XfsShutdown:
        drainTimeout: 30m
        for: 5m
      ReadonlyFilesystem:
        preMitigationJob:
          name: capture-node
    cooldown: 15m
    stallTimeout: 5m
    jobRetention: 1h
    excludeNamespaces:
      - kube-system
    excludeNodeLabels:
      node-role.kubernetes.io/infra: "true"
    excludeNodeTaints:
      - key: karpenter.sh/disrupted
      - key: ToBeDeletedByClusterAutoscaler
    excludeNodeAnnotations:
      karpenter.sh/do-not-disrupt: "true"
```

`terminate` and `capture-node` name templates in the [`jobs` map](jobs.md).
The controller skips a node if it matches any exclusion.
Excluding a namespace also excludes every node running a pod in that namespace.
Exclusions are checked only when a mitigation starts or adds the taint. A drain already in
progress also evicts pods in excluded namespaces.

| `app.config` field | Default | Meaning |
|---|---|---|
| `conditions` | `Manual: {}` | Map of Node condition types to settings below. Required when enabled; set an entry to `null` to remove it. |
| `cooldown` | `15m` | Minimum interval between new mitigations across the cluster. `taint: false` conditions are not counted. |
| `stallTimeout` | `5m` | Time past every remaining pod's deletion deadline before declaring a drain stalled. `0` disables detection. |
| `jobRetention` | `1h` | Time a finished Job is kept before the controller deletes it. A failed Job of a node's current mitigation is kept while the node exists; see [Job retention](jobs.md#job-retention). `0` deletes at once. |
| `excludeNamespaces` | Empty | Namespace names to exclude. |
| `excludeNodeLabels` | Empty | Exact label `key: value` pairs to exclude. |
| `excludeNodeTaints` | Empty | Taints to exclude, with required `key` and optional `value` and `effect`. |
| `excludeNodeAnnotations` | Empty | Exact annotation `key: value` pairs to exclude. |
| `drainRequests` | Empty | Annotates matching pods so another controller moves them, instead of evicting them. See [Drain requests](mitigation.md#drain-requests). |

Each `drainRequests` entry takes these fields. The first entry whose `podSelector` matches a pod applies to that pod:

| Field | Default | Meaning |
|---|---|---|
| `podSelector` | Required | Label selector for the pods to annotate. Must not be empty. |
| `annotationKey` | Required | Annotation key to set on each matching pod. |
| `annotationValue` | `"true"` | Annotation value to set. A pod that already has `annotationKey`, with any value, is not patched again. |
| `timeout` | `30m` | Time from the drain start before matching pods are evicted normally. `0` evicts at once. |

For example, a database operator that starts a replacement pod before it removes the old one:

```yaml
drainRequests:
  - podSelector:
      matchLabels:
        app.kubernetes.io/name: example-db
    annotationKey: example.com/drain-requested
    timeout: 30m
```

Each entry in `conditions` selects the [mitigation steps](mitigation.md#steps) for that condition:

| Field | Default | Meaning |
|---|---|---|
| `taint` | `true` | Applies the `NoSchedule` taint. `false` allows only a `preMitigationJob` with `Continue`. |
| `drain` | `true` | Evicts pods through the Eviction API. Requires `taint`. |
| `drainTimeout` | `-1` | Eviction time limit, after which remaining pods are deleted gracefully. `-1` (or omitted) never times out; `0` gives up at once. Requires `drain`. |
| `skipPodDeletion` | `false` | At the drain timeout, leaves the remaining pods and launches the mitigation Job. Requires `drainTimeout` and `mitigationJob`. |
| `preMitigationJob.name` | None | [Pre-mitigation Job](jobs.md#pre-mitigation-jobs) from `jobs`. |
| `preMitigationJob.failurePolicy` | `Continue` | `Continue` or `Block`; see [failure policies](jobs.md#pre-mitigation-jobs). |
| `mitigationJob.name` | None | [Mitigation Job](jobs.md#mitigation-jobs) from `jobs`, the last step. Requires `taint`. |
| `for` | `0` | Duration the condition must stay True before mitigation. |
| `jobEnv` | Empty | Environment variables for the Jobs this condition launches; see [Job environment](jobs.md#job-environment). |

`for`, `drainTimeout`, `stallTimeout`, and `jobRetention` accept durations such as `30m`, `4d`, and `1w`.
Unknown fields are rejected, so a misspelled step cannot fall back to its default.

An operator can start any `conditions` entry on a node by name, including one no node
reports, such as the default `Manual`. The `for` window does not apply.
See [manual mitigation](operations.md#manual-mitigation).

## Evaluate a condition

Add the condition with `taint: false` to see how often it fires before letting it act:

```yaml
conditions:
  NewCondition:
    taint: false
```

Matching nodes get a `MitigationStarted` Event and appear in
`node_mitigated_info{tainted="false"}` and `nodes_mitigated_total{tainted="false"}`;
nothing is tainted or evicted. The mitigation ends when the condition recovers.
Add a `preMitigationJob` to capture diagnostics; remove `taint: false` to start mitigating.

## Runtime ConfigMap

| ConfigMap key | Helm source | Purpose |
|---|---|---|
| `enabled` | `app.enabled` | Only the string `"true"` enables mitigation. |
| `config` | `app.config` | YAML containing conditions and global settings. |
| `jobs` | `jobs` | Optional map of Job names to templates. |

Configuration is decoded strictly: unknown keys are fatal. If the ConfigMap is
missing or `enabled` is not `"true"`, mitigation pauses, including work in progress.
Use the separate `enabled` key for an [emergency pause](operations.md#kill-switch).
