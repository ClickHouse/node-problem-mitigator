package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// mitigate decides on and advances the node's mitigation.
func (r *NodeReconciler) mitigate(ctx context.Context, node *corev1.Node, flipped map[string]bool, cfg Config) (ctrl.Result, error) {
	candidates, pending := eligibleConditions(node, cfg.Conditions, time.Now())
	owner, current, committed := committedMitigation(ctx, node)
	if !committed {
		return r.start(ctx, node, flipped, cfg, candidates, pending)
	}

	if !cfg.Enabled {
		recordSkip(logf.FromContext(ctx), activeWatchedFlips(node, flipped, cfg.Conditions), skipReasonDisabled,
			"Controller not enabled via ConfigMap, pausing mitigation", "node", node.Name, "condition", owner)
		return ctrl.Result{}, nil
	}

	var gated ctrl.Result
	if next, nextPlan := takeover(owner, current, candidates, cfg); nextPlan != current {
		res, ok, err := r.escalate(ctx, node, flipped, cfg, owner, current, next, nextPlan, candidates)
		if err != nil {
			return ctrl.Result{}, err
		}
		if ok {
			owner, current = next, nextPlan
		}
		gated = res
	}
	res, err := r.advance(ctx, node, owner, current, cfg)
	res.RequeueAfter = soonest(soonest(res.RequeueAfter, gated.RequeueAfter), pending)
	return res, err
}

// start begins a mitigation on a node without one. The owner is the condition
// an operator set in AnnotationMitigatedCondition, else the first eligible
// watched condition; the other eligible conditions then merge in.
func (r *NodeReconciler) start(ctx context.Context, node *corev1.Node, flipped map[string]bool, cfg Config, candidates []string, pending time.Duration) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	episodeFlips := activeWatchedFlips(node, flipped, cfg.Conditions)
	var owner string
	var p plan
	if name := node.Annotations[AnnotationMitigatedCondition]; name != "" {
		if cc, ok := cfg.Conditions[name]; ok {
			owner, p = name, planFor(cc)
			// An operator-set condition has no flip, so the annotation itself is the episode.
			episodeFlips = []string{name}
		}
	}
	if owner == "" && len(candidates) > 0 {
		owner, p = candidates[0], planFor(cfg.Conditions[candidates[0]])
	}
	if owner == "" {
		// Nothing to mitigate. Drop a dry-run gauge left by a recovered condition.
		nodeMitigatedInfo.DeletePartialMatch(prometheus.Labels{labelNodeName: node.Name, labelTainted: "false"})
		// Any flip still recorded here is waiting out its "for" window.
		waiting := activeWatchedFlips(node, flipped, cfg.Conditions)
		recordSkip(log, freshFlips(waiting, flipped), skipReasonPending,
			"Condition not sustained long enough, waiting", "node", node.Name, "retryAfter", pending)
		// Re-arm: the reconcile that ends the window must still count its decision.
		r.carryConditionFlips(node.Name, waiting)
		return ctrl.Result{RequeueAfter: pending}, nil
	}
	first := owner
	owner, p = takeover(owner, p, candidates, cfg)

	if !cfg.Enabled {
		recordSkip(log, episodeFlips, skipReasonDisabled, "Controller not enabled via ConfigMap, skipping mitigation", "node", node.Name, "condition", owner)
		return ctrl.Result{}, nil
	}
	// The cache can lag behind this controller's own commit. Committing again
	// would start a second mitigation with new Jobs.
	var latest corev1.Node
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(node), &latest); err != nil {
		reconcileErrorsTotal.WithLabelValues("get_node").Inc()
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if _, _, committed := committedMitigation(ctx, &latest); committed {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if res, ok := r.gate(ctx, node, flipped, cfg, episodeFlips, owner, p); !ok {
		res.RequeueAfter = soonest(res.RequeueAfter, pending)
		return res, nil
	}
	if err := r.commit(ctx, node, owner, p, cfg, append([]string{owner, first}, candidates...)); err != nil {
		reconcileErrorsTotal.WithLabelValues("commit_mitigation").Inc()
		return ctrl.Result{}, fmt.Errorf("committing mitigation on node %s: %w", node.Name, err)
	}
	res, err := r.advance(ctx, node, owner, p, cfg)
	res.RequeueAfter = soonest(res.RequeueAfter, pending)
	return res, err
}

// gate runs the checks a node passes before a mitigation starts, or before a
// running one starts tainting: exclusions, dry-run and, for a new taint, the
// global cooldown. It reports whether the node passed.
func (r *NodeReconciler) gate(ctx context.Context, node *corev1.Node, flipped map[string]bool, cfg Config, episodeFlips []string, condition string, p plan) (ctrl.Result, bool) {
	log := logf.FromContext(ctx)
	if hasExcludedEntry(node.Labels, cfg.ExcludeNodeLabels) {
		recordSkip(log, episodeFlips, skipReasonLabel, "Node excluded by label selector, skipping mitigation", "node", node.Name, "condition", condition)
		return ctrl.Result{}, false
	}
	// Node annotation exclusion (e.g. karpenter.sh/do-not-disrupt).
	if hasExcludedEntry(node.Annotations, cfg.ExcludeNodeAnnotations) {
		recordSkip(log, episodeFlips, skipReasonAnnotation, "Node excluded by annotation, skipping mitigation", "node", node.Name, "condition", condition)
		return ctrl.Result{}, false
	}
	// Node taint exclusion: the node is already being disrupted/drained by
	// another controller.
	if isExcludedByTaint(node, cfg.ExcludeNodeTaints) {
		recordSkip(log, episodeFlips, skipReasonTaint, "Node excluded by taint, skipping mitigation", "node", node.Name, "condition", condition)
		return ctrl.Result{}, false
	}
	// Namespace exclusion: skip if node runs workloads from protected namespaces.
	ns, hasExcluded, err := r.hasExcludedWorkloads(ctx, node, cfg.ExcludeNamespaces)
	if err != nil {
		log.Error(err, "Failed to check excluded workloads, skipping mitigation (fail-closed)", "node", node.Name)
		reconcileErrorsTotal.WithLabelValues("list_pods").Inc()
		// Re-record the consumed flips so the retry can still log/count the skip.
		for c := range flipped {
			r.recordConditionFlip(node.Name, c)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, false
	}
	if hasExcluded {
		recordSkip(log, episodeFlips, skipReasonNamespace, "Node runs workloads from excluded namespace, skipping mitigation", "node", node.Name, "condition", condition, "namespace", ns)
		return ctrl.Result{}, false
	}

	// Dry-run disables all actions.
	if r.DryRun {
		recordSkip(log, episodeFlips, skipReasonDryRun, "Would mitigate node", "node", node.Name, "condition", condition, "plan", p.String())
		nodeMitigatedInfo.WithLabelValues(node.Name, condition, "false").Set(1)
		return ctrl.Result{}, false
	}

	if !p.taint || hasTaint(node) {
		return ctrl.Result{}, true
	}
	// Global rate limit: at most one new taint per cooldown window.
	cooldown, _ := time.ParseDuration(cfg.Cooldown) // safe: validated by readConfig
	if !r.Timestamps.CanOperate(ctx, cooldown) {
		next := r.Timestamps.NextAllowed(ctx, cooldown)
		recordSkip(log, episodeFlips, skipReasonCooldown, "Skipping mitigation, global cooldown active", "node", node.Name, "condition", condition, "retryAfter", next)
		return ctrl.Result{RequeueAfter: next}, false
	}
	// Record cooldown BEFORE tainting — otherwise a taint+failed-persist leaves
	// no cooldown and NPD flapping can taint the entire fleet in seconds.
	// On persist failure the in-memory timestamp is rolled back so the cooldown
	// is not burned; we retry on a fixed interval like the other error paths.
	if err := r.Timestamps.Record(ctx); err != nil {
		log.Error(err, "Failed to persist cooldown timestamp, refusing to taint", "node", node.Name)
		reconcileErrorsTotal.WithLabelValues("persist_timestamp").Inc()
		return ctrl.Result{RequeueAfter: 30 * time.Second}, false
	}
	return ctrl.Result{}, true
}

// addTaint adds the NoSchedule taint unless the node has it, and reports
// whether it did.
func addTaint(node *corev1.Node) bool {
	if hasTaint(node) {
		return false
	}
	node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{Key: TaintKey, Value: TaintValue, Effect: TaintEffect})
	return true
}

// commit records the owner and plan on the node, taints it if the plan says
// so, and selects the pre-mitigation Job of the first of conditions that has
// one, all in one patch.
func (r *NodeReconciler) commit(ctx context.Context, node *corev1.Node, owner string, p plan, cfg Config, conditions []string) error {
	base := node.DeepCopy()
	tainted := p.taint && addTaint(node)
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	node.Annotations[AnnotationMitigatedAt] = time.Now().UTC().Format(time.RFC3339)
	node.Annotations[AnnotationMitigatedCondition] = owner
	node.Annotations[AnnotationMitigationPlan] = p.String()
	selectPreMitigationJob(node, cfg, conditions...)
	if err := r.Patch(ctx, node, client.StrategicMergeFrom(base)); err != nil {
		return err
	}

	note := fmt.Sprintf("Started mitigation for condition %s with plan %s", owner, p)
	if tainted {
		note += fmt.Sprintf("; applied taint %s=%s:%s", TaintKey, TaintValue, TaintEffect)
	}
	r.eventf(node, corev1.EventTypeNormal, eventReasonMitigationStarted, "Mitigate", "%s", note)
	logf.FromContext(ctx).Info("Started mitigation", "node", node.Name, "condition", owner, "plan", p.String())
	nodeMitigatedTotal.WithLabelValues(owner, strconv.FormatBool(p.taint)).Inc()
	// A dry-run gauge may have recorded the node until now.
	nodeMitigatedInfo.DeletePartialMatch(prometheus.Labels{labelNodeName: node.Name})
	if !p.drain && p.job == "" {
		r.observeMitigationDuration(node, owner)
	}
	return nil
}

// escalate hands the mitigation to a new owner, or applies a more aggressive
// plan from the owner's edited configuration. Turning the taint on passes the
// same gates as a new mitigation. It reports whether the node now runs the
// new plan.
func (r *NodeReconciler) escalate(ctx context.Context, node *corev1.Node, flipped map[string]bool, cfg Config, from string, fromPlan plan, to string, toPlan plan, candidates []string) (ctrl.Result, bool, error) {
	log := logf.FromContext(ctx)
	if toPlan.taint && !fromPlan.taint {
		if res, ok := r.gate(ctx, node, flipped, cfg, activeWatchedFlips(node, flipped, cfg.Conditions), to, toPlan); !ok {
			return res, false, nil
		}
	} else if r.DryRun {
		log.Info("Would escalate mitigation", "node", node.Name, "from", from, "to", to, "plan", toPlan.String())
		return ctrl.Result{}, false, nil
	}

	base := node.DeepCopy()
	tainted := toPlan.taint && addTaint(node)
	node.Annotations[AnnotationMitigatedCondition] = to
	node.Annotations[AnnotationMitigationPlan] = toPlan.String()
	selectPreMitigationJob(node, cfg, append([]string{to}, candidates...)...)
	if err := r.Patch(ctx, node, client.StrategicMergeFrom(base)); err != nil {
		reconcileErrorsTotal.WithLabelValues("record_escalation").Inc()
		return ctrl.Result{}, false, fmt.Errorf("escalating mitigation on node %s: %w", node.Name, err)
	}

	note := fmt.Sprintf("Escalated mitigation from condition %s to %s with plan %s", from, to, toPlan)
	if to == from {
		note = fmt.Sprintf("Escalated mitigation for condition %s to plan %s", to, toPlan)
	}
	if tainted {
		note += fmt.Sprintf("; applied taint %s=%s:%s", TaintKey, TaintValue, TaintEffect)
	}
	r.eventf(node, corev1.EventTypeNormal, eventReasonMitigationEscalated, "Mitigate", "%s", note)
	log.Info("Escalating mitigation", "node", node.Name, "from", from, "to", to, "plan", toPlan.String())
	// The previous owner's series would otherwise stay; advance sets the new ones.
	nodeMitigatedInfo.DeletePartialMatch(prometheus.Labels{labelNodeName: node.Name})
	drainStalledInfo.DeletePartialMatch(prometheus.Labels{labelNodeName: node.Name})
	if to != from || tainted {
		nodeMitigatedTotal.WithLabelValues(to, strconv.FormatBool(toPlan.taint)).Inc()
	}
	return ctrl.Result{}, true, nil
}

// advance runs the node's plan from where it stands: the pre-mitigation Job,
// the drain, then the mitigation Job. A mitigation that does not taint ends
// once its owner condition is no longer True and its pre-mitigation Job finished.
func (r *NodeReconciler) advance(ctx context.Context, node *corev1.Node, owner string, p plan, cfg Config) (ctrl.Result, error) {
	// Re-assert the in-memory gauge: a restart wipes it, the annotations persist.
	nodeMitigatedInfo.WithLabelValues(node.Name, owner, strconv.FormatBool(p.taint)).Set(1)
	if r.DryRun {
		return ctrl.Result{}, nil
	}
	if !p.taint && !conditionActive(node, owner) && preMitigationJobDone(node) {
		return r.end(ctx, node, owner)
	}

	decision, result, err := r.reconcilePreMitigationJob(ctx, node, p, cfg)
	if err != nil || decision != preMitigationJobProceed {
		return result, err
	}

	var end *drainEnd
	if p.drain {
		stallTimeout, _ := parseOptionalDuration(cfg.StallTimeout)
		if mitigationJobLaunched(node, p.job) {
			// The Job owns the node now; never resume pod removal.
			r.trackStallGauge(ctx, node, owner, cfg.ExcludeNamespaces, stallTimeout)
			return ctrl.Result{}, nil
		}
		res, drainEnd, done := r.drainStep(ctx, node, owner, p, stallTimeout, cfg)
		if !done {
			return res, nil
		}
		end = drainEnd
	}
	if p.job == "" {
		return ctrl.Result{}, nil
	}
	return r.launchPlanJob(ctx, node, owner, p.job, end, cfg), nil
}

// drainStep runs one drain pass. It reports whether the plan may move on to
// the mitigation Job and, when the drain gave up on pods, why. A stalled
// drain moves on only when there is a mitigation Job.
func (r *NodeReconciler) drainStep(ctx context.Context, node *corev1.Node, owner string, p plan, stallTimeout time.Duration, cfg Config) (ctrl.Result, *drainEnd, bool) {
	if _, drained := node.Annotations[AnnotationDrainFinishedAt]; drained {
		return ctrl.Result{}, nil, true
	}
	outcome, res := r.drainNode(ctx, node, owner, p.drainTimeout, stallTimeout, p.skipPodDeletion && p.job != "", cfg.ExcludeNamespaces, cfg.DrainRequests)
	switch outcome {
	case drainInProgress:
		return res, nil, false
	case drainSkipped:
		end := drainEndTimeout()
		return res, &end, true
	case drainStalled:
		if p.job == "" {
			if r.drainStalls.mark(node.UID, "") {
				r.reportDrainStalled(ctx, node, stallTimeout, cfg.ExcludeNamespaces)
			}
			return res, nil, false
		}
		// Waiting longer, or deleting the pods again, changes nothing.
		end := drainEndStalled(stallTimeout)
		return res, &end, true
	}
	if p.job == "" {
		r.observeMitigationDuration(node, owner)
		logf.FromContext(ctx).Info("Node drained", "node", node.Name, "condition", owner)
	}
	return ctrl.Result{}, nil, true
}

// launchPlanJob launches the plan's mitigation Job. end says why the drain
// left pods on the node; one Event names them after the launch.
func (r *NodeReconciler) launchPlanJob(ctx context.Context, node *corev1.Node, owner, job string, end *drainEnd, cfg Config) ctrl.Result {
	var undrained []string
	if end != nil && !mitigationJobLaunched(node, job) {
		// The pod names only feed the Event, so a failed list must not hold up the Job.
		pods, err := r.evictablePods(ctx, node, cfg.ExcludeNamespaces)
		if err != nil {
			logf.FromContext(ctx).Error(err, "Failed to list pods left on node, launching the mitigation Job anyway", "node", node.Name)
			reconcileErrorsTotal.WithLabelValues("list_pods_drain").Inc()
			r.eventf(node, corev1.EventTypeWarning, end.reason, "MitigationJob",
				"%s; could not list the pods left on the node, launching the mitigation Job anyway: %v", end.summary, err)
		}
		undrained = pods
	}
	launched, result := r.launchMitigationJob(ctx, node, owner, job, cfg)
	if launched && len(undrained) > 0 {
		r.eventf(node, corev1.EventTypeWarning, end.reason, "MitigationJob",
			"%s; left %d pod(s) on the node for the mitigation Job: %s",
			end.summary, len(undrained), joinPodRefs(undrained, maxEventPodListLen))
	}
	return result
}

// conditionActive reports whether the condition is True on the node.
func conditionActive(node *corev1.Node, condition string) bool {
	return !conditionTransitionTime(node, condition).IsZero()
}

// preMitigationJobDone reports whether the node has no pre-mitigation Job or
// it finished.
func preMitigationJobDone(node *corev1.Node) bool {
	return node.Annotations[AnnotationPreMitigationJob] == "" ||
		preMitigationJobFinished(node.Annotations[AnnotationPreMitigationJobOutcome])
}

// end closes a mitigation that does not taint. It clears the mitigation's
// annotations, gauges and in-memory dedups, so the next episode starts afresh.
func (r *NodeReconciler) end(ctx context.Context, node *corev1.Node, owner string) (ctrl.Result, error) {
	base := node.DeepCopy()
	for _, key := range [...]string{AnnotationMitigatedAt, AnnotationMitigatedCondition, AnnotationMitigationPlan,
		AnnotationDrainStartedAt, AnnotationDrainFinishedAt} {
		delete(node.Annotations, key)
	}
	for _, key := range preMitigationJobAnnotations {
		delete(node.Annotations, key)
	}
	if err := r.Patch(ctx, node, client.StrategicMergeFrom(base)); err != nil {
		reconcileErrorsTotal.WithLabelValues("end_mitigation").Inc()
		return ctrl.Result{}, fmt.Errorf("ending mitigation on node %s: %w", node.Name, err)
	}
	labels := prometheus.Labels{labelNodeName: node.Name}
	nodeMitigatedInfo.DeletePartialMatch(labels)
	drainStalledInfo.DeletePartialMatch(labels)
	jobFailedInfo.DeletePartialMatch(labels)
	r.forgetNode(node.UID)
	logf.FromContext(ctx).Info("Mitigation ended", "node", node.Name, "condition", owner)
	r.eventf(node, corev1.EventTypeNormal, eventReasonMitigationEnded, "Mitigate",
		"Ended mitigation for condition %s: the condition is no longer active and its Jobs finished", owner)
	return ctrl.Result{}, nil
}

// forgetNode drops the node's in-memory dedups.
func (r *NodeReconciler) forgetNode(uid types.UID) {
	r.launchingJobs.forget(uid)
	r.launchErrors.forget(uid)
	r.drainStalls.forget(uid)
	r.durationsObserved.forget(uid)
}
