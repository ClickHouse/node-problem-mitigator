# Jobs

Use Jobs to capture node details, call a cloud API, or run another recovery step.
Templates define each Job's commands, images, and permissions.

[Back to README](../README.md)

## Pre-mitigation Jobs

Define Job templates in Helm's `jobs` map and select one for a condition with
`preMitigationJob.name`. This `capture-node` example saves the Node object in logs:

```yaml
app:
  config:
    conditions:
      ReadonlyFilesystem:
        taint: false
        preMitigationJob:
          name: capture-node
          failurePolicy: Continue
jobs:
  capture-node:
    serviceAccount:
      name: capture-node
      clusterRules:
        - apiGroups: [""]
          resources: ["nodes"]
          verbs: ["get"]
    template:
      spec:
        activeDeadlineSeconds: 300
        backoffLimit: 1
        template:
          spec:
            containers:
              - name: capture
                image: registry.k8s.io/kubectl:v1.35.0
                args: ["get", "node", "$(NODE_PROBLEM_MITIGATOR_NODE_NAME)", "-o", "yaml"]
```

Like every mitigation step, Jobs run only with `app.enabled: true` and dry-run off.
With `taint: false`, the controller runs the Job and records its progress on the node, but does not taint it.
Such a condition skips the cooldown, so use its Job for diagnostics only.

Choose what happens if the Job fails with `preMitigationJob.failurePolicy`:

- `Continue` (default) proceeds with mitigation even if the Job fails.
- `Block` waits for an operator to retry the Job or continue past the failure. Requires `taint`.

Pre-mitigation Jobs run after any tainting, before eviction or the mitigation Job.
Time spent waiting for them does not count toward `drainTimeout`.
`activeDeadlineSeconds` defaults to 900 and cannot exceed 3600. If an unfinished Job disappears, the controller treats it as failed.

A mitigation runs at most one pre-mitigation Job: the owner's, or else the first one among the
other eligible conditions. When another condition [takes over](mitigation.md#ownership-and-escalation),
its pre-mitigation Job runs only if the node has not selected one yet.
Editing the template does not restart a Job that was already selected.
Use the [recovery controls](operations.md#pre-mitigation-job-recovery) to retry it or continue past a failure.

## Mitigation Jobs

Select a mitigation Job from the `jobs` map with `mitigationJob.name`. It is the last step:
it starts once the drain finishes, or right away when the condition does not drain.
A drain that times out with `skipPodDeletion`, or stalls, also starts it
(see [drain timeouts and stalls](mitigation.md#drain-timeouts-and-stalls)).
Once it launches, the controller stops removing pods.

A mitigation launches its mitigation Job once. Controller restarts and Job deletion do not
launch it again. The first condition to select a mitigation Job keeps it, even if a condition
with a different Job takes over later.

Templates define containers, retries, and deadlines. They cannot set `ttlSecondsAfterFinished`;
the controller deletes finished Jobs itself (see [Job retention](#job-retention)). The controller supplies
the Job name, namespace, identity metadata, and environment. `restartPolicy`
defaults to `Never`. Templates must contain at least one container.
A Job name in the `jobs` map has at most 34 characters: letters, digits, `-`, `_` and `.`,
starting and ending with a letter or digit. Condition names must be valid label values.

## Job outcomes

The controller watches its Jobs. When a Job finishes, `jobs_total` counts `succeeded`, `failed`,
or `deadline`; a Job deleted before it finishes counts `missing`. Outcomes are counted even after
the Job's node is gone, as when a terminate Job removes its own node. A Job that finishes while
no controller runs, during a restart or leader change, is not counted. Launches count `launched`,
or `launch_error` when the Job cannot be created.

The Node records the pre-mitigation Job's outcome, which decides whether the mitigation continues.
A failed mitigation Job sets `job_failed_info`, which fires `NodeProblemMitigatorJobFailed`, until
the Job is deleted. The controller does not retry failed mitigation Jobs; see [failed Jobs](operations.md#failed-jobs).

| Node annotation | Value |
|---|---|
| `clickhouse.com/node-pre-mitigation-job` | Selected `jobs` entry. |
| `clickhouse.com/node-pre-mitigation-job-condition` | Condition that selected it. |
| `clickhouse.com/node-pre-mitigation-job-attempt` | Attempt number; absent means 1. |
| `clickhouse.com/node-pre-mitigation-job-outcome` | `running`, then the outcome; absent until the Job is created. |
| `clickhouse.com/node-mitigation-job-launched-<job>` | Kubernetes name of the launched Job. |

Each Job carries these labels, and the `clickhouse.com/npm-target-node` annotation with the Node name:

| Job label | Value |
|---|---|
| `app.kubernetes.io/managed-by` | `node-problem-mitigator` |
| `app.kubernetes.io/component` | `pre-mitigation` or `mitigation` |
| `clickhouse.com/npm-job` | `jobs` entry. |
| `clickhouse.com/npm-mitigate-condition` | Condition that launched it. |
| `clickhouse.com/npm-target-node-uid` | Node UID. |

List a node's Jobs by its UID:

```sh
kubectl -n <namespace> get jobs -l clickhouse.com/npm-target-node-uid=$(kubectl get node <node> -o jsonpath='{.metadata.uid}')
```

### Job retention

The controller deletes a finished Job `jobRetention` (default `1h`) after it finishes. It keeps:

- the failed Job of the node's current mitigation, while the node exists, with its pod logs;
- a pre-mitigation Job whose outcome the Node has not recorded yet.

Deleting a Job never launches it again: the Node annotations record the launch. If the controller
is uninstalled, it leaves its Jobs behind; delete them with
`kubectl -n <namespace> delete jobs -l app.kubernetes.io/managed-by=node-problem-mitigator`.

## Cloud termination

The [AWS](../examples/aws.yaml), [GCP](../examples/gcp.yaml), and [Azure](../examples/azure.yaml)
examples enable mitigation, a pre-mitigation Job that captures the Node object, and alerts.
Before deploying them, configure identity for your cluster and review which conditions they watch.

The cloud examples define one `jobs.terminate` template and select it with `mitigationJob: {name: terminate}`.
It requests termination with a graceful guest OS shutdown, or without one when the condition's
`jobEnv` sets `FORCE_TERMINATE: "true"`, as the examples' `Manual` condition does.
The mitigator does not create replacement instances.

## ServiceAccounts and permissions

Configure a Job's identity with `serviceAccount` under its `jobs` entry.

| Field | Default | Purpose |
|---|---|---|
| `create` | `true` | Creates the account. With `false`, binds RBAC to an existing account. |
| `name` | `<release full name>-<Job name>` | Required when `create: false`. Must be a lowercase RFC 1123 subdomain, at most 253 characters. |
| `annotations` | `{}` | Cloud identity bindings. |
| `rules` | `[]` | Role rules in the release namespace. |
| `clusterRules` | `[]` | ClusterRole rules. |

The chart sets each Job's `serviceAccountName` from this block.
Omit the block to use the account named in the template, or use `{}` for the chart defaults.

A Job cannot use the controller's ServiceAccount. The example `terminate` Job uses
`node-mitigator-sa`, which needs cloud permissions but no Kubernetes RBAC.

Limit cloud permissions to this cluster's instances. Configure the identity to trust
the cluster issuer and `system:serviceaccount:<namespace>:<name>`.

| Cloud | Identity binding | Required permissions |
|---|---|---|
| AWS IRSA | `eks.amazonaws.com/role-arn` annotation | `ec2:TerminateInstances`, restricted to the cluster's instances. |
| GCP Workload Identity | `iam.gke.io/gcp-service-account` annotation | `compute.instances.get`, `compute.instances.list`, `compute.instances.delete`, `compute.zoneOperations.get`. `serviceAccount:<project>.svc.id.goog[<namespace>/<name>]` requires `roles/iam.workloadIdentityUser`. |
| Azure Workload Identity | `azure.workload.identity/client-id` annotation; tenant annotation for another tenant | On the node resource group: `Microsoft.Compute/virtualMachineScaleSets/read`, `Microsoft.Compute/virtualMachineScaleSets/virtualMachines/read`, `Microsoft.Compute/virtualMachineScaleSets/delete/action`, `Microsoft.Compute/virtualMachineScaleSets/virtualMachines/delete`. |

Azure also requires the Workload Identity webhook and the Job pod label
`azure.workload.identity/use: "true"`, as shown in the example.

## Job environment

A condition's `jobEnv` adds variables to every container and init container of the
pre-mitigation and mitigation Jobs it starts. They replace template values with the same names:

```yaml
app:
  config:
    conditions:
      Manual:
        mitigationJob:
          name: terminate
        jobEnv:
          FORCE_TERMINATE: "true"
```

The controller also sets these variables, replacing template values.
`jobEnv` names cannot start with `NODE_PROBLEM_MITIGATOR_`:

| Variable | Value |
|---|---|
| `NODE_PROBLEM_MITIGATOR_NODE_NAME` | Node name. |
| `NODE_PROBLEM_MITIGATOR_NODE_UID` | Node UID. |
| `NODE_PROBLEM_MITIGATOR_PROVIDER_ID` | Node `spec.providerID`. |
| `NODE_PROBLEM_MITIGATOR_REGION` | `topology.kubernetes.io/region` label, or empty. |
| `NODE_PROBLEM_MITIGATOR_ZONE` | `topology.kubernetes.io/zone` label, or empty. |
| `NODE_PROBLEM_MITIGATOR_CONDITION` | Condition that launched the Job. |

Container `args` support Kubernetes `$(VARIABLE)` expansion. In shell scripts, quote
environment variables, for example `"$NODE_PROBLEM_MITIGATOR_PROVIDER_ID"`.
Do not insert node metadata directly into script text; it can allow shell injection.

Restrict template editing to administrators. Anyone who can write this ConfigMap
can create Jobs using ServiceAccounts in the controller namespace.
