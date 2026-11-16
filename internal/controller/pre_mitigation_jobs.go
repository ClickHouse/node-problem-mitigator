package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Node annotations of the mitigation's pre-mitigation Job.
const (
	// AnnotationPreMitigationJob names the selected Job in the jobs map.
	AnnotationPreMitigationJob = "clickhouse.com/node-pre-mitigation-job"
	// AnnotationPreMitigationJobCondition is the condition that selected it.
	AnnotationPreMitigationJobCondition = "clickhouse.com/node-pre-mitigation-job-condition"
	// AnnotationPreMitigationJobAttempt counts retries; absent means 1.
	AnnotationPreMitigationJobAttempt = "clickhouse.com/node-pre-mitigation-job-attempt"
	// AnnotationPreMitigationJobOutcome is absent until the Job is created,
	// "running" while it runs, and then its final outcome.
	AnnotationPreMitigationJobOutcome = "clickhouse.com/node-pre-mitigation-job-outcome"
	// AnnotationPreMitigationJobAction is set by an operator: "retry" starts
	// the next attempt of a finished Job, "continue" proceeds past a failure.
	AnnotationPreMitigationJobAction = "clickhouse.com/node-pre-mitigation-job-action"

	preMitigationJobRunning        = "running"
	PreMitigationJobActionRetry    = "retry"
	PreMitigationJobActionContinue = "continue"

	eventReasonPreMitigationJobStarted   = "PreMitigationJobStarted"
	eventReasonPreMitigationJobFailed    = "PreMitigationJobFailed"
	eventReasonPreMitigationJobSucceeded = "PreMitigationJobSucceeded"
)

// preMitigationJobAnnotations are cleared when an observe-only mitigation ends.
var preMitigationJobAnnotations = [...]string{
	AnnotationPreMitigationJob, AnnotationPreMitigationJobCondition, AnnotationPreMitigationJobAttempt,
	AnnotationPreMitigationJobOutcome, AnnotationPreMitigationJobAction,
}

// preMitigationJobRef identifies the node's pre-mitigation Job in metrics.
func preMitigationJobRef(node *corev1.Node) jobRef {
	return jobRef{
		kind:      jobKindPreMitigation,
		condition: node.Annotations[AnnotationPreMitigationJobCondition],
		job:       node.Annotations[AnnotationPreMitigationJob],
	}
}

// clearPreMitigationJobFailed drops the node's pre-mitigation job_failed_info series.
func clearPreMitigationJobFailed(node *corev1.Node) {
	jobFailedInfo.DeletePartialMatch(prometheus.Labels{labelNodeName: node.Name, labelJobKind: jobKindPreMitigation})
}

type preMitigationJobDecision int

const (
	preMitigationJobProceed preMitigationJobDecision = iota
	preMitigationJobWait
	preMitigationJobBlock
)

func preMitigationJobAttempt(node *corev1.Node) int {
	attempt, err := strconv.Atoi(node.Annotations[AnnotationPreMitigationJobAttempt])
	if err != nil || attempt < 1 {
		return 1
	}
	return attempt
}

// preMitigationJobObjectName names the Job of the node's current pre-mitigation attempt.
func preMitigationJobObjectName(node *corev1.Node) string {
	return jobObjectName(node, preMitigationJobComponent, node.Annotations[AnnotationPreMitigationJob],
		node.Annotations[AnnotationPreMitigationJobCondition], preMitigationJobAttempt(node))
}

// preMitigationJobFinished reports whether the outcome is final.
func preMitigationJobFinished(outcome string) bool {
	return outcome != "" && outcome != preMitigationJobRunning
}

// selectPreMitigationJob records the pre-mitigation Job of the first condition
// that has one, unless the mitigation already has one. The caller patches the
// node.
func selectPreMitigationJob(node *corev1.Node, cfg Config, conditions ...string) {
	if node.Annotations[AnnotationPreMitigationJob] != "" {
		return
	}
	for _, condition := range conditions {
		cc := cfg.Conditions[condition]
		if cc.PreMitigationJob.Name != "" {
			node.Annotations[AnnotationPreMitigationJob] = cc.PreMitigationJob.Name
			node.Annotations[AnnotationPreMitigationJobCondition] = condition
			return
		}
	}
}

// preMitigationLaunchKey names the node's current pre-mitigation Job attempt in
// launchErrors. Job names cannot contain "/", so it never matches a mitigation Job.
func preMitigationLaunchKey(node *corev1.Node) string {
	return preMitigationJobComponent + "/" + node.Annotations[AnnotationPreMitigationJob] + "/" + strconv.Itoa(preMitigationJobAttempt(node))
}

// preMitigationJobBlocks reports whether a failed pre-mitigation Job stops the
// mitigation.
func preMitigationJobBlocks(node *corev1.Node, p plan) bool {
	return p.failurePolicy == PreMitigationJobFailureBlock && node.Annotations[AnnotationPreMitigationJobAction] != PreMitigationJobActionContinue
}

// reconcilePreMitigationJob advances the node's pre-mitigation Job and reports
// whether the mitigation may continue. It never recreates a Job after a known
// running Job disappears.
func (r *NodeReconciler) reconcilePreMitigationJob(ctx context.Context, node *corev1.Node, p plan, cfg Config) (preMitigationJobDecision, ctrl.Result, error) {
	name := node.Annotations[AnnotationPreMitigationJob]
	if name == "" {
		return preMitigationJobProceed, ctrl.Result{}, nil
	}

	outcome := node.Annotations[AnnotationPreMitigationJobOutcome]
	if preMitigationJobFinished(outcome) && node.Annotations[AnnotationPreMitigationJobAction] == PreMitigationJobActionRetry {
		if err := r.retryPreMitigationJob(ctx, node); err != nil {
			reconcileErrorsTotal.WithLabelValues("retry_pre_mitigation_job").Inc()
			return preMitigationJobWait, ctrl.Result{RequeueAfter: drainRequeueInterval}, err
		}
		outcome = ""
	}
	switch {
	case outcome == jobOutcomeSucceeded:
		return preMitigationJobProceed, ctrl.Result{}, nil
	case preMitigationJobFinished(outcome):
		// Restore the failure gauge after a controller restart.
		preMitigationJobRef(node).setFailed(node)
		if preMitigationJobBlocks(node, p) {
			return preMitigationJobBlock, ctrl.Result{}, nil
		}
		return preMitigationJobProceed, ctrl.Result{}, nil
	}

	condition := node.Annotations[AnnotationPreMitigationJobCondition]
	job, err := r.getPreMitigationJob(ctx, node)
	if errors.Is(err, errJobIdentity) {
		// A foreign Job holds the name: ours was never created, or was replaced.
		lost := jobOutcomeMissing
		if outcome == "" {
			lost = jobOutcomeLaunchError
		}
		return r.finishPreMitigationJob(ctx, node, p, lost, err.Error())
	}
	if err != nil && !apierrors.IsNotFound(err) {
		logf.FromContext(ctx).Error(err, "Failed to read pre-mitigation Job; will keep waiting", "node", node.Name, "preMitigationJob", name)
		reconcileErrorsTotal.WithLabelValues("get_pre_mitigation_job").Inc()
		return preMitigationJobWait, ctrl.Result{RequeueAfter: drainRequeueInterval}, nil
	}
	if outcome == "" {
		created := false
		if apierrors.IsNotFound(err) {
			jobConfig := cfg.Jobs[name]
			if jobConfig == nil {
				return r.finishPreMitigationJob(ctx, node, p, jobOutcomeLaunchError, "pre-mitigation Job configuration is no longer present")
			}
			desired := preMitigationJobFromTemplate(r.Namespace, jobConfig.Template)
			prepareJob(desired, node, preMitigationJobComponent, name, condition, preMitigationJobAttempt(node))
			injectJobEnvironment(desired, node, condition, cfg.Conditions[condition].JobEnv)
			job, created, err = r.createOrAdoptJob(ctx, desired)
			if err != nil {
				if apierrors.IsForbidden(err) || apierrors.IsInvalid(err) || errors.Is(err, errJobIdentity) {
					return r.finishPreMitigationJob(ctx, node, p, jobOutcomeLaunchError, err.Error())
				}
				logf.FromContext(ctx).Error(err, "Failed to create pre-mitigation Job; will retry", "node", node.Name, "preMitigationJob", name)
				if r.launchErrors.mark(node.UID, preMitigationLaunchKey(node)) {
					preMitigationJobRef(node).count(node.Name, jobOutcomeLaunchError)
				}
				reconcileErrorsTotal.WithLabelValues("launch_pre_mitigation_job").Inc()
				return preMitigationJobWait, ctrl.Result{RequeueAfter: drainRequeueInterval}, nil
			}
		}
		base := node.DeepCopy()
		node.Annotations[AnnotationPreMitigationJobOutcome] = preMitigationJobRunning
		if err := r.Patch(ctx, node, client.StrategicMergeFrom(base)); err != nil {
			logf.FromContext(ctx).Error(err, "Failed to persist running pre-mitigation Job", "node", node.Name, "preMitigationJob", name)
			reconcileErrorsTotal.WithLabelValues("annotate_pre_mitigation_job_running").Inc()
			return preMitigationJobWait, ctrl.Result{RequeueAfter: drainRequeueInterval}, nil
		}
		if created {
			preMitigationJobRef(node).count(node.Name, jobOutcomeLaunched)
			r.eventf(node, corev1.EventTypeNormal, eventReasonPreMitigationJobStarted, "PreMitigationJob",
				"Launched pre-mitigation Job %q: %s/%s", name, job.Namespace, job.Name)
		}
	} else if apierrors.IsNotFound(err) {
		return r.finishPreMitigationJob(ctx, node, p, jobOutcomeMissing, "the pre-mitigation Job is missing")
	}

	return r.observePreMitigationJob(ctx, node, p, job)
}

// observePreMitigationJob decides on a launched Job. A running Job is checked
// again on its next change or when its deadline passes.
func (r *NodeReconciler) observePreMitigationJob(ctx context.Context, node *corev1.Node, p plan, job *batchv1.Job) (preMitigationJobDecision, ctrl.Result, error) {
	switch outcome, detail := jobResult(job); outcome {
	case jobOutcomeSucceeded:
		return r.finishPreMitigationJob(ctx, node, p, outcome, "")
	case jobOutcomeFailed, jobOutcomeDeadline:
		return r.finishPreMitigationJob(ctx, node, p, outcome, "pre-mitigation Job "+detail)
	case jobOutcomeMissing:
		return r.finishPreMitigationJob(ctx, node, p, outcome, "the pre-mitigation Job is being deleted")
	}
	deadline := defaultPreMitigationJobDeadlineSeconds
	if job.Spec.ActiveDeadlineSeconds != nil && *job.Spec.ActiveDeadlineSeconds > 0 {
		deadline = min(*job.Spec.ActiveDeadlineSeconds, maxPreMitigationJobDeadlineSeconds)
	}
	started := job.CreationTimestamp.Time
	if job.Status.StartTime != nil {
		started = job.Status.StartTime.Time
	}
	remaining := time.Duration(deadline)*time.Second - time.Since(started)
	if remaining <= 0 {
		return r.finishPreMitigationJob(ctx, node, p, jobOutcomeDeadline, "pre-mitigation Job exceeded its deadline")
	}
	return preMitigationJobWait, ctrl.Result{RequeueAfter: remaining}, nil
}

// preMitigationJobFromTemplate copies the template and bounds its deadline.
func preMitigationJobFromTemplate(namespace string, template batchv1.JobTemplateSpec) *batchv1.Job {
	job := jobFromTemplate(namespace, template)
	if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds <= 0 {
		deadline := defaultPreMitigationJobDeadlineSeconds
		job.Spec.ActiveDeadlineSeconds = &deadline
	} else if *job.Spec.ActiveDeadlineSeconds > maxPreMitigationJobDeadlineSeconds {
		deadline := maxPreMitigationJobDeadlineSeconds
		job.Spec.ActiveDeadlineSeconds = &deadline
	}
	return job
}

// getPreMitigationJob reads the Job of the node's current pre-mitigation attempt.
func (r *NodeReconciler) getPreMitigationJob(ctx context.Context, node *corev1.Node) (*batchv1.Job, error) {
	want := &batchv1.Job{}
	prepareJob(want, node, preMitigationJobComponent, node.Annotations[AnnotationPreMitigationJob],
		node.Annotations[AnnotationPreMitigationJobCondition], preMitigationJobAttempt(node))
	var job batchv1.Job
	if err := r.jobReader().Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: want.Name}, &job); err != nil {
		return nil, err
	}
	if !sameJobIdentity(&job, want) {
		return nil, fmt.Errorf("%w: %s/%s", errJobIdentity, job.Namespace, job.Name)
	}
	return &job, nil
}

// finishPreMitigationJob stores the Job outcome on the node and reports the
// decision once. The Job watch counts Job outcomes; only a launch error is
// counted here.
func (r *NodeReconciler) finishPreMitigationJob(ctx context.Context, node *corev1.Node, p plan, outcome, reason string) (preMitigationJobDecision, ctrl.Result, error) {
	base := node.DeepCopy()
	node.Annotations[AnnotationPreMitigationJobOutcome] = outcome
	if err := r.Patch(ctx, node, client.StrategicMergeFrom(base)); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to persist pre-mitigation Job outcome", "node", node.Name)
		reconcileErrorsTotal.WithLabelValues("annotate_pre_mitigation_job_outcome").Inc()
		return preMitigationJobWait, ctrl.Result{RequeueAfter: drainRequeueInterval}, nil
	}

	ref := preMitigationJobRef(node)
	if outcome == jobOutcomeLaunchError && r.launchErrors.mark(node.UID, preMitigationLaunchKey(node)) {
		ref.count(node.Name, outcome)
	}
	if outcome != jobOutcomeSucceeded {
		ref.setFailed(node)
		r.eventf(node, corev1.EventTypeWarning, eventReasonPreMitigationJobFailed, "PreMitigationJob",
			"Pre-mitigation Job %q finished with outcome %s: %s", ref.job, outcome, reason)
		if preMitigationJobBlocks(node, p) {
			return preMitigationJobBlock, ctrl.Result{}, nil
		}
		return preMitigationJobProceed, ctrl.Result{}, nil
	}
	r.eventf(node, corev1.EventTypeNormal, eventReasonPreMitigationJobSucceeded, "PreMitigationJob",
		"Pre-mitigation Job %q succeeded", ref.job)
	return preMitigationJobProceed, ctrl.Result{}, nil
}

// retryPreMitigationJob starts the next attempt of a finished Job.
func (r *NodeReconciler) retryPreMitigationJob(ctx context.Context, node *corev1.Node) error {
	base := node.DeepCopy()
	node.Annotations[AnnotationPreMitigationJobAttempt] = strconv.Itoa(preMitigationJobAttempt(node) + 1)
	delete(node.Annotations, AnnotationPreMitigationJobOutcome)
	delete(node.Annotations, AnnotationPreMitigationJobAction)
	if err := r.Patch(ctx, node, client.StrategicMergeFrom(base)); err != nil {
		return err
	}
	clearPreMitigationJobFailed(node)
	return nil
}
