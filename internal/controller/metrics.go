package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Metric label names.
const (
	labelCondition  = "condition"
	labelTainted    = "tainted"
	labelNodeName   = "node_name"
	labelReason     = "reason"
	labelAction     = "action"
	labelOutcome    = "outcome"
	labelJobKind    = "kind"
	labelJob        = "job_name" // Not "job", which Prometheus sets to the scrape job.
	labelController = "controller"
)

// Action values for drain_pod_actions_total.
const (
	actionEvictPod     = "evict_pod"
	actionDeletePod    = "delete_pod"
	actionRequestDrain = "request_drain"
)

// Outcome values for the evict_pod action.
const (
	evictionOutcomeEvicted = "evicted" // Eviction API accepted, or pod already gone.
	evictionOutcomeBlocked = "blocked" // Rejected by a PodDisruptionBudget (429); retried.
	evictionOutcomeError   = "error"   // Any other eviction failure.
)

// Outcome values for the delete_pod action (drain escalated past its timeout).
const (
	deleteOutcomeDeleted = "deleted" // Delete accepted, or pod already gone.
	deleteOutcomeError   = "error"   // Delete failed.
)

// Outcome values for the request_drain action (pod annotated for another controller to move).
const (
	requestOutcomeRequested = "requested" // Annotation patch accepted.
	requestOutcomeError     = "error"     // Annotation patch failed.
)

// Skip reason values.
const (
	skipReasonCooldown   = "cooldown"
	skipReasonDryRun     = "dry_run"
	skipReasonDisabled   = "disabled"
	skipReasonLabel      = "label"
	skipReasonTaint      = "taint"
	skipReasonAnnotation = "annotation"
	skipReasonNamespace  = "namespace"
	// pending = the condition is True but still inside its "for" window.
	skipReasonPending = "pending"
)

var (
	nodeMitigatedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "node_problem_mitigator_nodes_mitigated_total",
		Help: "Total number of mitigations started or taken over, by condition and whether they taint the node.",
	}, []string{labelCondition, labelTainted})

	// nodeMitigatedInfo is 1 for each node under mitigation; bounded by the
	// number of bad nodes.
	nodeMitigatedInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "node_problem_mitigator_node_mitigated_info",
		Help:        "Set to 1 for each node under mitigation, by owning condition and whether the mitigation taints the node. tainted=false also covers dry-run. Removed when the node is deleted or a mitigation that does not taint ends.",
		ConstLabels: prometheus.Labels{labelController: controllerName},
	}, []string{labelNodeName, labelCondition, labelTainted})

	// drainStalledInfo is set to 1 for each node whose drain is stalled
	// Enables alerting on a stalled drain
	drainStalledInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "node_problem_mitigator_drain_stalled_info",
		Help:        "Set to 1 for each node whose drain is stalled: every remaining evictable pod has been terminating past its deletion deadline for longer than stallTimeout. Removed when the drain progresses or the node is deleted.",
		ConstLabels: prometheus.Labels{labelController: controllerName},
	}, []string{labelNodeName, labelCondition})

	// jobFailedInfo is set to 1 for each Job that ended without succeeding.
	// Enables alerting on failed Jobs.
	jobFailedInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "node_problem_mitigator_job_failed_info",
		Help:        "Set to 1 for each node whose Job failed. A mitigation Job that failed or exceeded its deadline is reported until the Job is deleted. A pre-mitigation Job that failed, exceeded its deadline, was deleted before it finished, or could not be created is reported until it is retried or a mitigation that does not taint ends. Removed when the node is deleted.",
		ConstLabels: prometheus.Labels{labelController: controllerName},
	}, []string{labelNodeName, labelCondition, labelJobKind, labelJob})

	mitigationSkippedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "node_problem_mitigator_mitigation_skipped_total",
		Help: "Total number of mitigation operations skipped, broken down by reason (cooldown, dry_run, disabled, label, taint, annotation, namespace, pending).",
	}, []string{labelCondition, labelReason})

	mitigationActionDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "node_problem_mitigator_mitigation_action_duration_seconds",
		Help:    "Time from acting on a node to its last step: the mitigation Job launch, else the drain completing, else the taint. Includes time blocked on PodDisruptionBudgets.",
		Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 1200, 1800, 3600},
	}, []string{labelCondition})

	mitigationTotalDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "node_problem_mitigator_mitigation_total_duration_seconds",
		Help:    "Time from the triggering condition becoming True to the mitigation's last step, including any time the node waited for the global cooldown before being acted on.",
		Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 1200, 1800, 3600},
	}, []string{labelCondition})

	drainPodActionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "node_problem_mitigator_drain_pod_actions_total",
		Help: "Total pod removals attempted by drains, by node, condition, action, and outcome: action=evict_pod (evicted, blocked, error), action=delete_pod (deleted, error) for each pod a drain deletes past its timeout, or action=request_drain (requested, error) for each pod a drain annotates for another controller to move.",
	}, []string{labelNodeName, labelCondition, labelAction, labelOutcome})

	reconcileErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "node_problem_mitigator_reconcile_errors_total",
		Help: "Total number of reconciliation errors.",
	}, []string{"reason"})

	jobsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "node_problem_mitigator_jobs_total",
		Help: "Total pre-mitigation and mitigation Job launches and outcomes by node, condition, kind, configured Job, and outcome (launched, launch_error, succeeded, failed, deadline, missing). Outcomes are counted when the controller sees the Job finish or be deleted; a Job that finishes while no controller runs is not counted.",
	}, []string{labelNodeName, labelCondition, labelJobKind, labelJob, labelOutcome})

	jobDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "node_problem_mitigator_job_duration_seconds",
		Help:    "Time from Job creation until the Job completed or failed, by kind and configured Job.",
		Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 1200, 1800, 3600},
	}, []string{labelJobKind, labelJob})

	// conditionRecoveryTime is observed at the True->False edge of a watched condition.
	// Buckets span 10s (one node-status update) to ~3.8d, since a condition that
	// does not taint can stay True for days before recovering.
	conditionRecoveryTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "node_problem_mitigator_condition_recovery_time_seconds",
		Help:    "Time a watched condition stayed True before recovering, by condition.",
		Buckets: prometheus.ExponentialBuckets(10, 2, 16),
	}, []string{labelCondition})

	// buildInfo is the controller's liveness signal: 1 while running, absent when not.
	buildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_problem_mitigator_build_info",
		Help: "Always 1 while the controller is running and exporting metrics; the version label carries the build. Use its presence per cluster to detect instances that are not up.",
	}, []string{"version"})
)

func init() {
	metrics.Registry.MustRegister(
		nodeMitigatedTotal,
		nodeMitigatedInfo,
		drainStalledInfo,
		jobFailedInfo,
		mitigationSkippedTotal,
		mitigationActionDuration,
		mitigationTotalDuration,
		drainPodActionsTotal,
		reconcileErrorsTotal,
		jobsTotal,
		jobDuration,
		conditionRecoveryTime,
		buildInfo,
	)
}

// SetBuildInfo publishes the build_info liveness gauge for the given version.
func SetBuildInfo(version string) {
	buildInfo.WithLabelValues(version).Set(1)
}

// seedMetrics creates the configured conditions' and Jobs' series at 0. A
// series born at its first observation starts at 1, which increase() and
// rate() cannot see.
func seedMetrics(cfg Config) {
	for condition, cc := range cfg.Conditions {
		conditionRecoveryTime.WithLabelValues(condition)
		mitigationActionDuration.WithLabelValues(condition)
		mitigationTotalDuration.WithLabelValues(condition)
		nodeMitigatedTotal.WithLabelValues(condition, "true").Add(0)
		nodeMitigatedTotal.WithLabelValues(condition, "false").Add(0)
		for _, reason := range []string{skipReasonCooldown, skipReasonDryRun,
			skipReasonDisabled, skipReasonLabel, skipReasonTaint, skipReasonAnnotation, skipReasonNamespace,
			skipReasonPending} {
			mitigationSkippedTotal.WithLabelValues(condition, reason).Add(0)
		}
		if cc.PreMitigationJob.Name != "" {
			jobDuration.WithLabelValues(jobKindPreMitigation, cc.PreMitigationJob.Name)
		}
		if cc.MitigationJob.Name != "" {
			jobDuration.WithLabelValues(jobKindMitigation, cc.MitigationJob.Name)
		}
	}
}
