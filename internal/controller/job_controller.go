package controller

import (
	"context"
	"errors"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// jobRecheckInterval is how often a kept Job is checked for deletion.
const jobRecheckInterval = 10 * time.Minute

// JobReconciler counts each Job's outcome as it happens and deletes finished
// Jobs after jobRetention.
type JobReconciler struct {
	client.Client
	Namespace string
	// Recorder emits mitigation Job outcome Events on the target node, or on
	// the Job once the node is gone.
	Recorder events.EventRecorder
}

// SetupWithManager sets up the controller with the Manager.
func (r *JobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.Job{}, builder.WithPredicates(r.outcomeRecorder())).
		Named("job").
		Complete(r)
}

// outcomeRecorder filters nothing; it reports a Job's outcome when the Job
// finishes, or when it is deleted before finishing. A Job already finished
// when first seen, as after a restart, is not reported again.
func (r *JobReconciler) outcomeRecorder() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldJob, okOld := e.ObjectOld.(*batchv1.Job)
			newJob, okNew := e.ObjectNew.(*batchv1.Job)
			if !okOld || !okNew {
				return true
			}
			if before, _ := jobResult(oldJob); before == "" {
				if outcome, detail := jobResult(newJob); outcome != "" {
					r.reportOutcome(newJob, outcome, detail)
				}
			}
			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			if job, ok := e.Object.(*batchv1.Job); ok {
				if outcome, _ := jobResult(job); outcome == "" {
					r.reportOutcome(job, jobOutcomeMissing, "was deleted before it finished")
				}
			}
			return true
		},
	}
}

// reportOutcome counts the outcome, observes the duration and logs it. A
// mitigation Job also gets an Event; a pre-mitigation Job's Event comes with
// the node's decision.
func (r *JobReconciler) reportOutcome(job *batchv1.Job, outcome, detail string) {
	ref := jobRefOf(job)
	nodeName := job.Annotations[jobTargetNodeAnnotation]
	ref.count(nodeName, outcome)
	if finished := jobFinishedAt(job); !finished.IsZero() {
		jobDuration.WithLabelValues(ref.kind, ref.job).Observe(finished.Sub(job.CreationTimestamp.Time).Seconds())
	}
	log := logf.Log.WithName("job-outcomes").WithValues("node", nodeName, "condition", ref.condition,
		"kind", ref.kind, "job", ref.job, "jobName", job.Name, "outcome", outcome)
	if outcome == jobOutcomeSucceeded {
		log.Info("Job succeeded")
	} else {
		log.Error(errors.New(detail), "Job did not succeed")
	}
	if ref.kind != jobKindMitigation {
		return
	}
	var regarding runtime.Object = job
	var node corev1.Node
	if err := r.Get(context.Background(), client.ObjectKey{Name: nodeName}, &node); err == nil && string(node.UID) == job.Labels[jobTargetUIDLabel] {
		regarding = &node
	}
	if outcome == jobOutcomeSucceeded {
		recordEvent(r.Recorder, regarding, corev1.EventTypeNormal, eventReasonMitigationJobSucceeded, "MitigationJob",
			"Mitigation Job %q (%s/%s) for condition %s succeeded", ref.job, job.Namespace, job.Name, ref.condition)
		return
	}
	recordEvent(r.Recorder, regarding, corev1.EventTypeWarning, eventReasonMitigationJobFailed, "MitigationJob",
		"Mitigation Job %q (%s/%s) for condition %s %s", ref.job, job.Namespace, job.Name, ref.condition, detail)
}

// Reconcile deletes a finished Job once jobRetention has passed, unless keepJob holds it.
func (r *JobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var job batchv1.Job
	if err := r.Get(ctx, req.NamespacedName, &job); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	outcome, _ := jobResult(&job)
	if outcome == "" || outcome == jobOutcomeMissing {
		// Running, or already being deleted.
		return ctrl.Result{}, nil
	}
	keep, err := r.keepJob(ctx, &job, outcome)
	if err != nil {
		reconcileErrorsTotal.WithLabelValues("get_job_node").Inc()
		return ctrl.Result{}, err
	}
	if keep {
		return ctrl.Result{RequeueAfter: jobRecheckInterval}, nil
	}
	cfg := readConfig(ctx, r.Client, r.Namespace)
	if wait := cfg.jobRetention() - time.Since(jobFinishedAt(&job)); wait > 0 {
		return ctrl.Result{RequeueAfter: wait}, nil
	}
	if err := r.Delete(ctx, &job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
		reconcileErrorsTotal.WithLabelValues("delete_job").Inc()
		return ctrl.Result{}, err
	}
	logf.FromContext(ctx).Info("Deleted finished Job", "job", job.Name, "node", job.Annotations[jobTargetNodeAnnotation], "outcome", outcome)
	return ctrl.Result{}, nil
}

// keepJob reports whether a finished Job must stay past jobRetention: it is
// the current Job of its node and either failed or, for a pre-mitigation Job,
// its outcome is not yet recorded on the node.
func (r *JobReconciler) keepJob(ctx context.Context, job *batchv1.Job, outcome string) (bool, error) {
	var node corev1.Node
	if err := r.Get(ctx, client.ObjectKey{Name: job.Annotations[jobTargetNodeAnnotation]}, &node); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if string(node.UID) != job.Labels[jobTargetUIDLabel] {
		return false, nil
	}
	name := job.Labels[jobNameLabel]
	switch job.Labels[jobComponentLabel] {
	case preMitigationJobComponent:
		current := node.Annotations[AnnotationPreMitigationJob] == name && preMitigationJobObjectName(&node) == job.Name
		return current && (outcome != jobOutcomeSucceeded || !preMitigationJobFinished(node.Annotations[AnnotationPreMitigationJobOutcome])), nil
	case mitigationJobComponent:
		current := node.Annotations[mitigationJobLaunchedAnnotation(name)] == job.Name
		return current && outcome != jobOutcomeSucceeded, nil
	}
	return false, nil
}
