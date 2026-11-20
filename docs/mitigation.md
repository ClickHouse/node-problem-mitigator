# Mitigation behavior

A watched Node condition must stay True for its configured `for` duration before mitigation starts.

[Back to README](../README.md)

## Steps

A mitigation runs these steps in order. Each condition [configures](configuration.md#conditions-and-exclusions) which of them run:

| Step | Field | Default |
|---|---|---|
| Apply the `clickhouse.com/node-problem-detector-bad-node:true:NoSchedule` taint | `taint` | On |
| Run the [pre-mitigation Job](jobs.md#pre-mitigation-jobs) | `preMitigationJob` | None |
| Evict the node's pods | `drain` | On |
| Run the [mitigation Job](jobs.md#mitigation-jobs) | `mitigationJob` | None |

The pre-mitigation Job gates every later step. A condition with `taint: false`
only runs its pre-mitigation Job.

The node records the condition that owns the mitigation in
`clickhouse.com/node-mitigated-condition` and the steps it runs in
`clickhouse.com/node-mitigation-plan`, for example
`taint,drain,drainTimeout=30m,job=terminate`. When reading it, the controller also accepts
`key=value` for every step (`taint=false`) and fills an absent step with its configuration default.

## Global cooldown

The cooldown allows one new mitigation per window across the cluster, 15 minutes by default.
Later steps of a started mitigation do not wait for it. A condition with `taint: false` only
runs a pre-mitigation Job, so it is not counted.

The controller saves the start time in a ConfigMap before tainting the node, so a failed save
prevents the mitigation.
Leader election keeps one controller active.
Dry-run does not launch Jobs or change nodes.

## Ownership and escalation

When several conditions are eligible, their steps merge, each setting keeping its most aggressive value:

| Setting | Merged value |
|---|---|
| `taint`, `drain`, `skipPodDeletion` | On if any condition turns it on. |
| `drainTimeout` | The shortest. |
| Pre-mitigation Job `failurePolicy` | `Block` only if every condition with a pre-mitigation Job blocks. |
| `mitigationJob` | The first one selected; it never changes. |

A started mitigation never becomes less aggressive. On every check the controller merges the
owner's current configuration and every other eligible condition into the recorded steps.
A condition whose steps change the result becomes the owner. The owner's `jobEnv` applies to
Jobs launched afterwards, and metrics carry its name. Editing a condition to be less
aggressive does not affect running mitigations.

An escalation that adds the taint counts as a new mitigation: it passes the same exclusions,
dry-run and cooldown. A drain already in progress keeps its start time, so its timeout is not reset.

A mitigation that taints the node continues after its condition recovers or its configuration
changes, until the node is deleted. Use the [kill switch](operations.md#kill-switch) or dry-run
to pause it. A mitigation that does not taint ends once its owner condition is no longer True
and its pre-mitigation Job has finished. The controller then removes its node annotations, so the next
episode runs the Job again.

## Pod eviction

The drain evicts pods through the Eviction API, which respects PodDisruptionBudgets.
DaemonSet, completed and mirror pods remain on the node.

The controller retries until all evictable pods are gone. It then records
`clickhouse.com/node-mitigation-drain-finished-at` and launches the mitigation Job, if any.
Without a mitigation Job, the node stays tainted. Once a mitigation Job launches, the controller
stops removing pods.

Check Pod Events for successful evictions (`Evicted`) and deletions after timeout
(`DrainTimeout`). Check Node Events for blocked or failed evictions.

## Drain requests

With `drainRequests` set, the drain does not evict a pod that matches an entry's `podSelector`.
The first matching entry applies to the pod. The drain sets the entry's `annotationKey: annotationValue`
on the pod, emits `DrainRequested`, and waits for another controller to move the pod. A pod that
already has `annotationKey`, with any value, is not patched again. The pod still counts as remaining,
so the drain waits for it.
A matching pod is removed in this order:

1. The drain sets the request annotation.
2. After the entry's `timeout` from the drain start, the drain evicts the pod normally.
3. After the condition's `drainTimeout`, the drain deletes the pod, as for any other pod.

If the entry's `timeout` is longer than `drainTimeout`, step 2 never occurs. Failed annotation
patches emit `DrainRequestFailed` on the Node and are retried on the next pass.

## Drain timeouts and stalls

By default a drain waits indefinitely. `drainTimeout` limits it, counting from the first drain
pass, after any pre-mitigation Job. `0` gives up at once. After a timeout, the drain deletes
remaining pods with their grace periods, bypassing PodDisruptionBudgets, then waits for them to
leave. With `skipPodDeletion: true` it instead leaves the remaining pods in place and launches
the mitigation Job.

[![Drain timeout animation](terminate-timeout.svg#play-0-a)][drain-timeout-animation]

A drain is stalled when every remaining evictable pod is more than `stallTimeout` past its
deletion deadline. A pod's deadline is its deletion time plus its grace period.
The default is `5m`; set `stallTimeout: 0` to disable detection.
A stalled drain launches the mitigation Job, leaving the pods in place, regardless of
`drainTimeout` or `skipPodDeletion`. Without a mitigation Job it emits `DrainStalled` and waits
for an operator to intervene.

[![Drain stall animation](terminate-stall.svg#play-0-a)][drain-stall-animation]

The controller does not force-delete pods. Removing a pod object while its process still runs
can create duplicate StatefulSet replicas and cause split-brain. After a mitigation Job
terminates the instance, cloud-controller-manager deletes the Node. Kubernetes then cleans up
the remaining pod objects.

## Termination

The controller has no built-in termination. A mitigation Job removes the instance, such as the
`terminate` Job in the [cloud examples](jobs.md#cloud-termination).
The condition's [`jobEnv`](jobs.md#job-environment) sets environment variables for the Job.

A [failed Job](jobs.md#job-outcomes) leaves the node drained and tainted. Fixing its template
does not relaunch it; terminate the instance directly instead.
[Stuck-node alerts](operations.md#stuck-mitigation) report tainted nodes that remain.

## Lifecycle diagrams

- [Reconcile overview](reconcile-overview.svg): safety gates, ownership, and continuation.
- [Detailed reconcile flow](https://raw.githubusercontent.com/ClickHouse/node-problem-mitigator/refs/heads/main/docs/reconcile-flow.svg): decisions and error paths.
- [Standard mitigation animation](https://raw.githubusercontent.com/ClickHouse/node-problem-mitigator/refs/heads/main/docs/terminate-standard.svg): pre-mitigation Job, graceful drain, and terminate Job.
- [Drain timeout animation][drain-timeout-animation]: graceful pod deletion before termination.
- [Drain stall animation][drain-stall-animation]: termination with pods left in place.

[drain-timeout-animation]: https://raw.githubusercontent.com/ClickHouse/node-problem-mitigator/refs/heads/main/docs/terminate-timeout.svg
[drain-stall-animation]: https://raw.githubusercontent.com/ClickHouse/node-problem-mitigator/refs/heads/main/docs/terminate-stall.svg
