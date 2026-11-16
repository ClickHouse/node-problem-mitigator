package controller

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	// TaintKey is applied to nodes detected as unhealthy by Node Problem Detector.
	TaintKey    = "clickhouse.com/node-problem-detector-bad-node"
	TaintValue  = "true"
	TaintEffect = corev1.TaintEffectNoSchedule

	// AnnotationMitigatedAt records when the mitigator acted on the node.
	AnnotationMitigatedAt = "clickhouse.com/node-mitigated-at"
	// AnnotationMitigatedCondition records the condition that owns the
	// mitigation. Operators set it to a configured condition to start a
	// mitigation by hand or to hand a running one over.
	AnnotationMitigatedCondition = "clickhouse.com/node-mitigated-condition"
	// AnnotationMitigationPlan records the steps the node runs (see plan). The
	// controller owns it: the steps only grow, so a mitigation never steps back.
	AnnotationMitigationPlan  = "clickhouse.com/node-mitigation-plan"
	AnnotationDrainFinishedAt = "clickhouse.com/node-mitigation-drain-finished-at"
	// AnnotationDrainStartedAt records when the drain began its first pass.
	// Pre-mitigation Job execution happens before this timestamp, so the wait is
	// not included in the drain timeout. A takeover keeps it.
	AnnotationDrainStartedAt = "clickhouse.com/node-mitigation-drain-started-at"

	eventReasonMitigationStarted   = "MitigationStarted"
	eventReasonMitigationEscalated = "MitigationEscalated"
	eventReasonMitigationEnded     = "MitigationEnded"
)

// nodeOnce is a set of names per node UID, safe for concurrent use.
type nodeOnce struct {
	mu   sync.Mutex
	seen map[nodeKey]struct{}
}

type nodeKey struct {
	uid  types.UID
	name string
}

// mark records the name for the node and reports whether it was new.
func (o *nodeOnce) mark(uid types.UID, name string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	key := nodeKey{uid: uid, name: name}
	if _, ok := o.seen[key]; ok {
		return false
	}
	if o.seen == nil {
		o.seen = make(map[nodeKey]struct{})
	}
	o.seen[key] = struct{}{}
	return true
}

// unmark removes the name for the node.
func (o *nodeOnce) unmark(uid types.UID, name string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.seen, nodeKey{uid: uid, name: name})
}

// forget removes every name for the node.
func (o *nodeOnce) forget(uid types.UID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for key := range o.seen {
		if key.uid == uid {
			delete(o.seen, key)
		}
	}
}

// NodeReconciler watches Node objects and mitigates nodes that Node Problem Detector
// has marked as unhealthy.
type NodeReconciler struct {
	client.Client
	Timestamps *TimestampStore
	DryRun     bool
	Namespace  string
	// APIReader is a non-cached client.Reader used for on-demand pod listing.
	// It goes directly to the API server, avoiding the need for a pod watch/cache.
	APIReader client.Reader
	// Recorder emits lifecycle and failure Events on the Node, surfaced via
	// `kubectl describe node`.
	Recorder events.EventRecorder

	// The sets below make an action run once per node. They live in memory, so
	// a controller restart may repeat each action once.
	//
	// launchingJobs holds mitigation Job launches until the launch annotation
	// is recorded. A restart that relaunches adopts the same Job.
	launchingJobs nodeOnce
	// launchErrors holds the Jobs whose launch failed, so jobs_total counts
	// launch_error, and a missing configuration logs and emits an Event, once.
	launchErrors nodeOnce
	// drainStalls holds nodes whose stalled drain has emitted DrainStalled.
	drainStalls nodeOnce
	// durationsObserved holds nodes whose mitigation duration is observed.
	durationsObserved nodeOnce

	// conditionFlipsMu guards conditionFlips.
	conditionFlipsMu sync.Mutex
	// conditionFlips holds, per node, the condition types that transitioned to
	// True since the node's last reconcile, recorded by the watch predicate and
	// consumed by Reconcile. Skip metrics and logs fire only on these
	// transitions, so unrelated node updates cannot re-count an episode. A false
	// value marks a flip re-armed across a "for" window: its pending skip is
	// already counted.
	conditionFlips map[string]map[string]bool

	// watchedConditionsMu guards watchedConditions.
	watchedConditionsMu sync.Mutex
	// watchedConditions is the set of condition types configured in the
	// ConfigMap, refreshed on every reconcile. The watch predicate reads it to
	// scope recovery telemetry to watched conditions. Empty until the first
	// reconcile.
	watchedConditions map[string]struct{}

	// pendingMetricCleanupsMu guards pendingMetricCleanups.
	pendingMetricCleanupsMu sync.Mutex
	// pendingMetricCleanups tracks when each deleted node was first seen missing;
	// its metric series are deleted metricsCleanupGracePeriod later so the final
	// increments still get scraped.
	pendingMetricCleanups map[string]time.Time
}

// metricsCleanupGracePeriod is how long a deleted node's metric series are
// kept, covering a few scrape intervals.
const metricsCleanupGracePeriod = 2 * time.Minute

// scheduleNodeMetricsCleanup deletes the node's per-node metric series once
// metricsCleanupGracePeriod has passed since the node was first seen deleted.
// Returns how long to requeue, or 0 once cleaned up.
func (r *NodeReconciler) scheduleNodeMetricsCleanup(name string) time.Duration {
	r.pendingMetricCleanupsMu.Lock()
	defer r.pendingMetricCleanupsMu.Unlock()
	firstSeen, seen := r.pendingMetricCleanups[name]
	if !seen {
		if r.pendingMetricCleanups == nil {
			r.pendingMetricCleanups = make(map[string]time.Time)
		}
		r.pendingMetricCleanups[name] = time.Now()
		return metricsCleanupGracePeriod
	}
	if remaining := metricsCleanupGracePeriod - time.Since(firstSeen); remaining > 0 {
		return remaining
	}
	delete(r.pendingMetricCleanups, name)
	deleteNodeMetricSeries(name)
	return 0
}

// deleteNodeMetricSeries removes every per-node metric series for the node name.
func deleteNodeMetricSeries(name string) {
	nodeMitigatedInfo.DeletePartialMatch(prometheus.Labels{labelNodeName: name})
	drainStalledInfo.DeletePartialMatch(prometheus.Labels{labelNodeName: name})
	jobFailedInfo.DeletePartialMatch(prometheus.Labels{labelNodeName: name})
	drainPodActionsTotal.DeletePartialMatch(prometheus.Labels{labelNodeName: name})
	jobsTotal.DeletePartialMatch(prometheus.Labels{labelNodeName: name})
}

func (r *NodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var node corev1.Node
	if err := r.Get(ctx, req.NamespacedName, &node); err != nil {
		if apierrors.IsNotFound(err) {
			// Node deleted — clean up its per-node series, but only after a grace
			// period so the final increments are scraped before the series vanish.
			return ctrl.Result{RequeueAfter: r.scheduleNodeMetricsCleanup(req.Name)}, nil
		}
		reconcileErrorsTotal.WithLabelValues("get_node").Inc()
		return ctrl.Result{}, err
	}

	// A previous node with this name was deleted and its metric cleanup is still
	// pending: purge those series now so the live node inherits no stale gauge.
	r.pendingMetricCleanupsMu.Lock()
	_, hadPendingCleanup := r.pendingMetricCleanups[req.Name]
	delete(r.pendingMetricCleanups, req.Name)
	r.pendingMetricCleanupsMu.Unlock()
	if hadPendingCleanup {
		deleteNodeMetricSeries(req.Name)
	}

	// Condition transitions to True since the node's last reconcile, recorded
	// by the watch predicate. Skip metrics and logs fire only for these.
	flipped := r.consumeConditionFlips(node.Name)

	// Read configuration from ConfigMap. Crash on invalid config.
	cfg := readConfig(ctx, r.Client, r.Namespace)
	r.snapshotWatchedConditions(cfg)

	seedMetrics(cfg)

	var reported ctrl.Result
	if !r.DryRun {
		reported = r.reportFailedMitigationJobs(ctx, &node)
	}
	result, err := r.mitigate(ctx, &node, flipped, cfg)
	result.RequeueAfter = soonest(result.RequeueAfter, reported.RequeueAfter)
	return result, err
}

// SetupWithManager sets up the controller with the Manager. A change to one of
// the controller's Jobs reconciles its target node.
func (r *NodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}, builder.WithPredicates(r.conditionFlipRecorder())).
		Watches(&batchv1.Job{}, handler.EnqueueRequestsFromMapFunc(targetNodeRequest)).
		Named("node").
		Complete(r)
}

// conditionFlipRecorder filters nothing; it records node condition transitions
// to True for the skip metric/log gate. For watched conditions it also logs
// each transition and observes the recovery time.
func (r *NodeReconciler) conditionFlipRecorder() predicate.Funcs {
	log := logf.Log.WithName("node-conditions")
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldNode, okOld := e.ObjectOld.(*corev1.Node)
			newNode, okNew := e.ObjectNew.(*corev1.Node)
			if !okOld || !okNew {
				return true
			}
			old := make(map[corev1.NodeConditionType]corev1.NodeCondition, len(oldNode.Status.Conditions))
			for _, c := range oldNode.Status.Conditions {
				old[c.Type] = c
			}
			for _, c := range newNode.Status.Conditions {
				watched := r.isWatchedCondition(string(c.Type))
				switch {
				case c.Status == corev1.ConditionTrue && old[c.Type].Status != corev1.ConditionTrue:
					r.recordConditionFlip(newNode.Name, string(c.Type))
					if watched {
						log.Info("Node condition became true",
							"node", newNode.Name, "condition", c.Type, "reason", c.Reason)
					}
				case c.Status != corev1.ConditionTrue && old[c.Type].Status == corev1.ConditionTrue:
					if !watched {
						continue
					}
					// Unknown means the node stopped reporting, not that it healed.
					if c.Status != corev1.ConditionFalse {
						continue
					}
					// The old transition time is when it became True; same-second flips give 0s.
					duration := c.LastTransitionTime.Sub(old[c.Type].LastTransitionTime.Time)
					if duration >= 0 {
						conditionRecoveryTime.WithLabelValues(string(c.Type)).Observe(duration.Seconds())
					}
					log.Info("Node condition recovered",
						"node", newNode.Name, "condition", c.Type, "duration", duration)
				}
			}
			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			r.consumeConditionFlips(e.Object.GetName())
			r.forgetNode(e.Object.GetUID())
			return true
		},
	}
}

// snapshotWatchedConditions refreshes the watched-condition set the predicate
// reads.
func (r *NodeReconciler) snapshotWatchedConditions(cfg Config) {
	set := make(map[string]struct{}, len(cfg.Conditions))
	for condition := range cfg.Conditions {
		set[condition] = struct{}{}
	}
	r.watchedConditionsMu.Lock()
	r.watchedConditions = set
	r.watchedConditionsMu.Unlock()
}

// isWatchedCondition reports whether the condition type is configured.
func (r *NodeReconciler) isWatchedCondition(condition string) bool {
	r.watchedConditionsMu.Lock()
	defer r.watchedConditionsMu.Unlock()
	_, ok := r.watchedConditions[condition]
	return ok
}

// recordSkip logs and counts a skipped mitigation, once per watched condition
// that just transitioned to True; reconciles with no new transition stay silent.
func recordSkip(log logr.Logger, episodeFlips []string, reason, msg string, kv ...any) {
	if len(episodeFlips) == 0 {
		return
	}
	log.Info(msg, kv...)
	for _, c := range episodeFlips {
		mitigationSkippedTotal.WithLabelValues(c, reason).Inc()
	}
}

// activeWatchedFlips returns the watched conditions with an open episode that
// are still True on the node.
func activeWatchedFlips(node *corev1.Node, flipped map[string]bool, conditions map[string]ConditionConfig) []string {
	var out []string
	for _, c := range node.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		if _, open := flipped[string(c.Type)]; !open {
			continue
		}
		if _, watched := conditions[string(c.Type)]; !watched {
			continue
		}
		out = append(out, string(c.Type))
	}
	return out
}

// freshFlips returns the flips no reconcile has counted as pending yet, so an
// episode counts one pending skip however many heartbeats it spans.
func freshFlips(conditions []string, flipped map[string]bool) []string {
	var out []string
	for _, c := range conditions {
		if flipped[c] {
			out = append(out, c)
		}
	}
	return out
}

// carryConditionFlips keeps the episode open past a pending skip. A flip the
// predicate recorded in the meantime stays fresh.
func (r *NodeReconciler) carryConditionFlips(nodeName string, conditions []string) {
	if len(conditions) == 0 {
		return
	}
	r.conditionFlipsMu.Lock()
	defer r.conditionFlipsMu.Unlock()
	if r.conditionFlips == nil {
		r.conditionFlips = make(map[string]map[string]bool)
	}
	if r.conditionFlips[nodeName] == nil {
		r.conditionFlips[nodeName] = make(map[string]bool, len(conditions))
	}
	for _, c := range conditions {
		if _, recorded := r.conditionFlips[nodeName][c]; !recorded {
			r.conditionFlips[nodeName][c] = false
		}
	}
}

// recordConditionFlip notes that the node condition transitioned to True.
func (r *NodeReconciler) recordConditionFlip(nodeName, condition string) {
	r.conditionFlipsMu.Lock()
	defer r.conditionFlipsMu.Unlock()
	if r.conditionFlips == nil {
		r.conditionFlips = make(map[string]map[string]bool)
	}
	if r.conditionFlips[nodeName] == nil {
		r.conditionFlips[nodeName] = make(map[string]bool)
	}
	r.conditionFlips[nodeName][condition] = true
}

// consumeConditionFlips returns and clears the node's recorded transitions.
func (r *NodeReconciler) consumeConditionFlips(nodeName string) map[string]bool {
	r.conditionFlipsMu.Lock()
	defer r.conditionFlipsMu.Unlock()
	flips := r.conditionFlips[nodeName]
	delete(r.conditionFlips, nodeName)
	return flips
}

// eligibleConditions returns the watched conditions that are True and past
// their "for" window, in name order, plus the delay until the soonest pending
// window elapses.
func eligibleConditions(node *corev1.Node, conditions map[string]ConditionConfig, now time.Time) ([]string, time.Duration) {
	var eligible []string
	var pending time.Duration
	for _, c := range node.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		cc, ok := conditions[string(c.Type)]
		if !ok {
			continue
		}
		if remaining := sustainRemaining(cc, c, now); remaining > 0 {
			pending = soonest(pending, remaining)
			continue
		}
		eligible = append(eligible, string(c.Type))
	}
	slices.Sort(eligible)
	return eligible, pending
}

// sustainRemaining returns how much longer the condition must stay True to
// satisfy its configured "for", or 0 once it already has. readConfig rejects an
// unparseable "for", so the ignored error here cannot hide one.
func sustainRemaining(cc ConditionConfig, c corev1.NodeCondition, now time.Time) time.Duration {
	d, _ := parseOptionalDuration(cc.For)
	if d <= 0 || c.LastTransitionTime.IsZero() {
		return 0
	}
	return max(d-now.Sub(c.LastTransitionTime.Time), 0)
}

// soonest returns the earlier of two requeue delays, treating 0 as "no requeue".
func soonest(a, b time.Duration) time.Duration {
	if a <= 0 || (b > 0 && b < a) {
		return b
	}
	return a
}

// committedMitigation returns the owner and plan recorded on the node, and
// whether a mitigation is committed. Both annotations are needed; an
// unparseable plan is logged and treated as no commitment.
func committedMitigation(ctx context.Context, node *corev1.Node) (string, plan, bool) {
	owner := node.Annotations[AnnotationMitigatedCondition]
	value, ok := node.Annotations[AnnotationMitigationPlan]
	if owner == "" || !ok {
		return "", plan{}, false
	}
	p, err := parsePlan(value)
	if err != nil {
		logf.FromContext(ctx).Error(err, "Ignoring unparseable mitigation plan", "node", node.Name, "annotation", AnnotationMitigationPlan)
		return "", plan{}, false
	}
	return owner, p, true
}

// hasTaint returns true if the node already has our taint.
func hasTaint(node *corev1.Node) bool {
	for _, t := range node.Spec.Taints {
		if t.Key == TaintKey && t.Effect == TaintEffect {
			return true
		}
	}
	return false
}

// hasExcludedEntry returns true if any exclude key: value pair is present in
// entries (node labels or annotations).
func hasExcludedEntry(entries, exclude map[string]string) bool {
	for k, v := range exclude {
		if val, ok := entries[k]; ok && val == v {
			return true
		}
	}
	return false
}

// isExcludedByTaint returns true if any node taint matches any of the exclude
// taints. Key must match exactly; value and effect are compared only when set
// in the exclude entry.
func isExcludedByTaint(node *corev1.Node, excludeTaints []ExcludeTaint) bool {
	for _, et := range excludeTaints {
		for _, t := range node.Spec.Taints {
			if t.Key != et.Key {
				continue
			}
			if et.Value != "" && t.Value != et.Value {
				continue
			}
			if et.Effect != "" && string(t.Effect) != et.Effect {
				continue
			}
			return true
		}
	}
	return false
}

// hasExcludedWorkloads returns the first excluded namespace found on the node,
// and true, if the node is running any pod from a namespace in excludeNamespaces.
// Returns ("", false, nil) if no excluded workloads are found or no namespaces are configured.
// Returns a non-nil error on API failures; the caller should fail closed.
// Uses the non-cached APIReader to avoid maintaining a cluster-wide pod watch.
func (r *NodeReconciler) hasExcludedWorkloads(ctx context.Context, node *corev1.Node, excludeNamespaces []string) (string, bool, error) {
	if len(excludeNamespaces) == 0 {
		return "", false, nil
	}

	var podList corev1.PodList
	if err := r.APIReader.List(ctx, &podList, client.MatchingFields{podNodeNameField: node.Name}); err != nil {
		return "", false, fmt.Errorf("listing pods on node %s: %w", node.Name, err)
	}

	for i := range podList.Items {
		if slices.Contains(excludeNamespaces, podList.Items[i].Namespace) {
			return podList.Items[i].Namespace, true, nil
		}
	}
	return "", false, nil
}

// observeMitigationDuration records two histograms, once per mitigation, when
// it reaches its last step: the action duration (from when the node was acted
// on, AnnotationMitigatedAt) and the total duration (from when the triggering
// condition became True).
func (r *NodeReconciler) observeMitigationDuration(node *corev1.Node, condition string) {
	if !r.durationsObserved.mark(node.UID, "") {
		return
	}
	if mitigatedAt, err := time.Parse(time.RFC3339, node.Annotations[AnnotationMitigatedAt]); err == nil {
		mitigationActionDuration.WithLabelValues(condition).Observe(time.Since(mitigatedAt).Seconds())
	}
	if detectedAt := conditionTransitionTime(node, condition); !detectedAt.IsZero() {
		mitigationTotalDuration.WithLabelValues(condition).Observe(time.Since(detectedAt).Seconds())
	}
}

// conditionTransitionTime returns the LastTransitionTime of the named node
// condition while it is currently True, or the zero time if no such condition
// is currently True. A condition that has since recovered (flipped to False)
// carries its recovery time, which would understate the total duration, so it
// is treated as absent.
func conditionTransitionTime(node *corev1.Node, conditionType string) time.Time {
	for _, c := range node.Status.Conditions {
		if string(c.Type) == conditionType && c.Status == corev1.ConditionTrue {
			return c.LastTransitionTime.Time
		}
	}
	return time.Time{}
}
