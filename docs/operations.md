# Operations

Use the kill switch to pause mitigation across the cluster, or Node annotations
to change how a single node is handled. Check metrics and Events for progress
and failures.

[Back to README](../README.md)

## Kill switch

Pause new and in-progress mitigations:

```sh
kubectl -n <namespace> patch configmap node-problem-mitigator-config \
  --type merge -p '{"data":{"enabled":"false"}}'
```

Dry-run also stops node changes and new Job launches. Jobs already running keep running;
delete one to stop it, which records it as `missing`. Ensure GitOps tools do not restore
`enabled: "true"` while the pause is needed.

## Manual mitigation

Start a mitigation on one node by setting any configured condition. The chart's
default `Manual` condition taints and drains:

```sh
kubectl annotate node "$NODE" clickhouse.com/node-mitigated-condition=Manual --overwrite
```

On a node without a mitigation, the condition's steps merge with any active
condition (see [ownership](mitigation.md#ownership-and-escalation)) and pass the
same exclusions, dry-run and cooldown as any mitigation. On a node already under
mitigation, the annotation hands the mitigation to that condition: its steps merge
into the running ones, which never become less aggressive.

Confirm the mitigation started and see its steps:

```sh
kubectl get node "$NODE" -o jsonpath='{.metadata.annotations.clickhouse\.com/node-mitigated-condition}{"\n"}{.metadata.annotations.clickhouse\.com/node-mitigation-plan}{"\n"}'
```

## Pre-mitigation Job recovery

The node records its pre-mitigation Job in `clickhouse.com/node-pre-mitigation-job`
(the `jobs` entry), `-condition`, `-attempt` and `-outcome`. After fixing a
template or permissions, start a new attempt with the current template:

```sh
kubectl annotate node "$NODE" clickhouse.com/node-pre-mitigation-job-action=retry --overwrite
```

Or let a blocked mitigation continue past the failure:

```sh
kubectl annotate node "$NODE" clickhouse.com/node-pre-mitigation-job-action=continue --overwrite
```

The controller removes `retry` once the new attempt starts; `continue` stays.
The kill switch and dry-run still apply.

## Metrics and Events

All metric names below include the `node_problem_mitigator_` prefix. Const
label `controller` is present on the node and Job gauges as noted.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `nodes_mitigated_total` | Counter | `condition`, `tainted` | Mitigations started or taken over, by condition and whether they taint the node. |
| `node_mitigated_info` | Gauge | `node_name`, `condition`, `tainted`; const `controller` | 1 for each node under mitigation; `tainted=false` includes dry-run. Removed on node deletion or when an untainted mitigation ends. |
| `drain_stalled_info` | Gauge | `node_name`, `condition`; const `controller` | 1 when every remaining evictable pod has been terminating past its deletion deadline longer than `stallTimeout`. Removed when drain progresses or node is deleted. |
| `job_failed_info` | Gauge | `node_name`, `condition`, `kind`, `job_name`; const `controller` | 1 while a mitigation Job that failed or exceeded its deadline exists, or after a pre-mitigation Job failed, exceeded its deadline, disappeared before finishing, or could not be created. Removed when the mitigation Job is deleted, the pre-mitigation Job is retried, an untainted mitigation ends, or the node is deleted. |
| `mitigation_skipped_total` | Counter | `condition`, `reason` | Skipped operations by reason: `cooldown`, `dry_run`, `disabled`, `label`, `taint`, `annotation`, `namespace`, or `pending`. |
| `mitigation_action_duration_seconds` | Histogram | `condition` | Time from acting on a node to its last step: mitigation Job launch, else drain completion, else taint; includes PDB waits. Observed once per mitigation. |
| `mitigation_total_duration_seconds` | Histogram | `condition` | Time from triggering condition becoming True to the last mitigation step, including cooldown. |
| `drain_pod_actions_total` | Counter | `node_name`, `condition`, `action`, `outcome` | Pod removals: `evict_pod` outcomes `evicted`, `blocked`, `error`; `delete_pod` outcomes `deleted`, `error`; `request_drain` outcomes `requested`, `error`. |
| `reconcile_errors_total` | Counter | `reason` | Reconciliation errors. |
| `jobs_total` | Counter | `node_name`, `condition`, `kind`, `job_name`, `outcome` | Pre-mitigation and mitigation Job launches and outcomes: `launched`, `launch_error`, `succeeded`, `failed`, `deadline`, `missing`. Outcomes count when the controller sees a Job finish or be deleted, also after its node is gone; a Job that finishes while no controller runs is not counted. Launch retries count `launch_error` once per node and Job. |
| `job_duration_seconds` | Histogram | `kind`, `job_name` | Time from Job creation until completion or failure, observed with the outcome. |
| `condition_recovery_time_seconds` | Histogram | `condition` | Time a watched condition stayed True before recovering. |
| `build_info` | Gauge | `version` | 1 while the controller runs and exports metrics. |

A skipped decision is counted once while a condition stays active, rather than
on every check. A skipped manual request counts on every check until it starts.
See Node Events for drain, pre-mitigation Job, and mitigation Job progress.

## Alerts

Set `serviceMonitor.enabled: true` and `prometheusRule.enabled: true` to enable
monitoring through the Prometheus Operator. The chart skips these resources if
their CRDs are missing. The fleet-size alert also requires kube-state-metrics.

Both resources have `release: kube-prometheus-stack` label by default, which kube-prometheus-stack
selects when installed under that release name. For another release name, set
`additionalLabels.release` on both. Any other Prometheus must select both
resources and be allowed to read the release namespace.

| Alert | Trigger | Default `for` | Severity |
|---|---|---|---|
| `NodeProblemMitigatorTooManyMitigatedNodes` | More than 20 nodes carry the bad-node taint. | `15m` | `critical` |
| `NodeProblemMitigatorNodeStuck` | A tainted node remains under mitigation (`node_mitigated_info{tainted="true"}`). | `4h` | `critical` |
| `MitigationDrainStalled` | A node has stalled pods. | `10m` | `warning` |
| `NodeProblemMitigatorJobFailed` | `node_problem_mitigator_job_failed_info` is set for a Job. | `0m` | `warning` |
| `NodeProblemMitigatorReconcileErrors` | More than 5 errors share a reason within 1 hour. | `15m` | `warning` |

Each built-in alert accepts `enabled`, `for`, `severity`, `labels`, and
`annotations`. `extraAlerts` adds custom rules as written, without the shared
labels, severity, or annotations. See the [cloud examples](../examples/).

Shared `prometheusRule.labels`, per-alert labels, and per-condition labels for
`NodeProblemMitigatorNodeStuck` are rendered as Helm templates, so values can
reference `.Values` and `.Release` (for example,
`team: '{{ .Values.global.clusterAdmin | default "sre" }}'`). Annotations and
`extraAlerts` are left unchanged.

`NodeProblemMitigatorNodeStuck` also accepts `conditions`, keyed by condition
name, for conditions whose mitigations take a different time to finish. Each
entry gets its own rule with its settings over the alert's, and the shared rule
skips that condition. `enabled: false` mutes the condition; a `null` entry falls
back to the shared rule:

```yaml
prometheusRule:
  alerts:
    NodeProblemMitigatorNodeStuck:
      for: 2h
      conditions:
        KernelDeadlock:
          for: 30m
        ReadonlyFilesystem:
          for: 4h
          severity: warning
```

## Stuck mitigation

`NodeProblemMitigatorNodeStuck` means a tainted node remains under mitigation
beyond the configured alert duration. Inspect its owner and plan, Node Events,
and Jobs:

```sh
kubectl get node "$NODE" -o yaml
kubectl describe node "$NODE"
kubectl -n <namespace> get jobs -l clickhouse.com/npm-target-node-uid=$(kubectl get node "$NODE" -o jsonpath='{.metadata.uid}')
```

A node with no mitigation Job may still be draining or blocked on a
pre-mitigation Job. A running Job may be waiting on the cloud API. The
cloud-controller-manager normally deletes a node after instance termination.

`MitigationDrainStalled` means terminating pods have passed their deletion
deadlines. Check the Node's `DrainStalled` Event and inspect those pods. The
controller does not force-delete pods.

## Failed Jobs

`NodeProblemMitigatorJobFailed` fires when `node_problem_mitigator_job_failed_info`
reports a failed, expired, missing, or uncreatable Job. The controller keeps the
failed Job of a node's current mitigation, with its pod logs, while the node exists
(see [Job retention](jobs.md#job-retention)). Find the Job and its Events, then
inspect its logs:

```sh
kubectl get node "$NODE" -o jsonpath='{.metadata.annotations}'
kubectl get events -A --field-selector involvedObject.name="$NODE"
kubectl -n <namespace> get jobs -l clickhouse.com/npm-target-node-uid=$(kubectl get node "$NODE" -o jsonpath='{.metadata.uid}')
kubectl -n <namespace> logs job/<job-name>
```

Fix the selected template or its identity. Mitigation Jobs are not retried
automatically; delete the failed mitigation Job to clear the alert. Pre-mitigation
Jobs can be retried with the [operator action](#pre-mitigation-job-recovery).

## Reconciliation errors and missing metrics

The `reason` label identifies the failing step; check controller logs for the
full error. Common causes include missing Kubernetes RBAC permissions and API
server errors. If the controller is running but `build_info` is absent, check
Prometheus discovery and scraping. Otherwise inspect pod status, Events, and logs.
