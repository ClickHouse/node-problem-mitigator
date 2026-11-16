package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Job metadata. The labels identify a Job and select it; the target node's
// name is an annotation because it can exceed the label value limit.
const (
	jobManagedByLabel       = "app.kubernetes.io/managed-by"
	jobComponentLabel       = "app.kubernetes.io/component"
	jobNameLabel            = "clickhouse.com/npm-job"
	jobConditionLabel       = "clickhouse.com/npm-mitigate-condition"
	jobTargetUIDLabel       = "clickhouse.com/npm-target-node-uid"
	jobTargetNodeAnnotation = "clickhouse.com/npm-target-node"

	preMitigationJobComponent = "pre-mitigation"
	mitigationJobComponent    = "mitigation"
)

// jobIdentityLabels are the labels a Job must carry to be adopted.
var jobIdentityLabels = [...]string{jobManagedByLabel, jobComponentLabel, jobNameLabel, jobConditionLabel, jobTargetUIDLabel}

// annotationMitigationJobLaunchedPrefix, followed by the Job's key in the jobs
// map, records the launched mitigation Job's name on the node.
const annotationMitigationJobLaunchedPrefix = "clickhouse.com/node-mitigation-job-launched-"

var errJobIdentity = errors.New("job has a different mitigation identity")

// jobObjectName is stable for a node UID, mitigation and attempt, allowing
// adoption after a controller restart or an API response lost after Job
// creation. A later mitigation of the same node gets new names. Pre-mitigation
// Job names omit the Job key to leave room for the attempt.
func jobObjectName(node *corev1.Node, component, name, condition string, attempt int) string {
	identity := string(node.UID)
	if identity == "" {
		identity = node.Name
	}
	identity += "\x00" + condition + "\x00" + node.Annotations[AnnotationMitigatedAt]
	digest := sha256.Sum256([]byte(identity))
	prefix := component
	if component == mitigationJobComponent {
		prefix += "-" + name
	}
	return fmt.Sprintf("%s-%s-%d", sanitizeJobPart(prefix), hex.EncodeToString(digest[:])[:10], attempt)
}

func sanitizeJobPart(value string) string {
	var b strings.Builder
	for _, c := range strings.ToLower(value) {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			b.WriteRune(c)
		} else {
			b.WriteByte('-')
		}
	}
	part := strings.Trim(b.String(), "-")
	if part == "" {
		return "job"
	}
	return part
}

// prepareJob names the Job and sets the metadata that identifies it. The
// caller supplies the complete Pod template and container command.
func prepareJob(job *batchv1.Job, node *corev1.Node, component, name, condition string, attempt int) {
	if job.Labels == nil {
		job.Labels = map[string]string{}
	}
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Name = jobObjectName(node, component, name, condition, attempt)
	job.GenerateName = ""
	job.Labels[jobManagedByLabel] = controllerName
	job.Labels[jobComponentLabel] = component
	job.Labels[jobNameLabel] = name
	job.Labels[jobConditionLabel] = condition
	job.Labels[jobTargetUIDLabel] = string(node.UID)
	job.Annotations[jobTargetNodeAnnotation] = node.Name
}

// jobFromTemplate copies a configured Job template into namespace. The Pod
// restartPolicy defaults to Never, since a Job rejects the Pod default Always.
func jobFromTemplate(namespace string, template batchv1.JobTemplateSpec) *batchv1.Job {
	job := &batchv1.Job{
		ObjectMeta: *template.ObjectMeta.DeepCopy(),
		Spec:       *template.Spec.DeepCopy(),
	}
	job.Namespace = namespace
	if job.Spec.Template.Spec.RestartPolicy == "" {
		job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
	}
	return job
}

// jobReader reads Jobs from the API server, so a Job created moments ago is
// never mistaken for a deleted one.
func (r *NodeReconciler) jobReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// createOrAdoptJob creates a deterministic Job or adopts the existing Job with
// the same identity. A failed Create is followed by a Get to handle lost replies.
func (r *NodeReconciler) createOrAdoptJob(ctx context.Context, desired *batchv1.Job) (*batchv1.Job, bool, error) {
	reader := r.jobReader()
	var existing batchv1.Job
	key := types.NamespacedName{Namespace: desired.Namespace, Name: desired.Name}
	if err := reader.Get(ctx, key, &existing); err == nil {
		if !sameJobIdentity(&existing, desired) {
			return nil, false, fmt.Errorf("%w: %s/%s", errJobIdentity, desired.Namespace, desired.Name)
		}
		return &existing, false, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, false, err
	}

	if err := r.Create(ctx, desired); err == nil {
		return desired, true, nil
	} else {
		if getErr := reader.Get(ctx, key, &existing); getErr == nil {
			if !sameJobIdentity(&existing, desired) {
				return nil, false, fmt.Errorf("%w after Create error: %s/%s", errJobIdentity, desired.Namespace, desired.Name)
			}
			return &existing, false, nil
		} else if !apierrors.IsNotFound(getErr) {
			return nil, false, getErr
		}
		return nil, false, err
	}
}

func sameJobIdentity(existing, desired *batchv1.Job) bool {
	for _, key := range jobIdentityLabels {
		if existing.Labels[key] != desired.Labels[key] {
			return false
		}
	}
	return existing.Annotations[jobTargetNodeAnnotation] == desired.Annotations[jobTargetNodeAnnotation]
}

// injectJobEnvironment gives each Job container the condition's jobEnv and the
// node context, replacing template variables of the same name. Kubernetes
// expands the node-name variable in command and args fields.
func injectJobEnvironment(job *batchv1.Job, node *corev1.Node, condition string, jobEnv map[string]string) {
	env := make([]corev1.EnvVar, 0, len(jobEnv)+6)
	for _, name := range slices.Sorted(maps.Keys(jobEnv)) {
		env = append(env, corev1.EnvVar{Name: name, Value: jobEnv[name]})
	}
	env = append(env,
		corev1.EnvVar{Name: "NODE_PROBLEM_MITIGATOR_NODE_NAME", Value: node.Name},
		corev1.EnvVar{Name: "NODE_PROBLEM_MITIGATOR_NODE_UID", Value: string(node.UID)},
		corev1.EnvVar{Name: "NODE_PROBLEM_MITIGATOR_PROVIDER_ID", Value: node.Spec.ProviderID},
		corev1.EnvVar{Name: "NODE_PROBLEM_MITIGATOR_REGION", Value: node.Labels[corev1.LabelTopologyRegion]},
		corev1.EnvVar{Name: "NODE_PROBLEM_MITIGATOR_ZONE", Value: node.Labels[corev1.LabelTopologyZone]},
		corev1.EnvVar{Name: "NODE_PROBLEM_MITIGATOR_CONDITION", Value: condition},
	)
	add := func(containers []corev1.Container) {
		for i := range containers {
			for _, injected := range env {
				filtered := containers[i].Env[:0]
				for _, current := range containers[i].Env {
					if current.Name != injected.Name {
						filtered = append(filtered, current)
					}
				}
				containers[i].Env = append(filtered, injected)
			}
		}
	}
	add(job.Spec.Template.Spec.InitContainers)
	add(job.Spec.Template.Spec.Containers)
}

// targetNodeRequest maps a Job to a reconcile request for its target node.
func targetNodeRequest(_ context.Context, obj client.Object) []reconcile.Request {
	name := obj.GetAnnotations()[jobTargetNodeAnnotation]
	if name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: name}}}
}

// Job kinds, the kind label of jobs_total and job_failed_info.
const (
	jobKindPreMitigation = "pre_mitigation"
	jobKindMitigation    = "mitigation"
)

// Job outcomes.
const (
	jobOutcomeLaunched    = "launched"     // Job created.
	jobOutcomeLaunchError = "launch_error" // Job could not be created.
	jobOutcomeSucceeded   = "succeeded"    // Job completed.
	jobOutcomeFailed      = "failed"       // Job failed.
	jobOutcomeDeadline    = "deadline"     // Job exceeded its deadline.
	jobOutcomeMissing     = "missing"      // Job deleted before it finished.
)

const (
	eventReasonMitigationJobLaunched     = "MitigationJobLaunched"
	eventReasonMitigationJobLaunchFailed = "MitigationJobLaunchFailed"
	eventReasonMitigationJobSucceeded    = "MitigationJobSucceeded"
	eventReasonMitigationJobFailed       = "MitigationJobFailed"
)

// jobRef holds the metric labels of a node's Job; job is its key in the jobs map.
type jobRef struct {
	kind, condition, job string
}

// mitigationJobRef identifies a mitigation Job launched for the condition.
func mitigationJobRef(condition, job string) jobRef {
	return jobRef{kind: jobKindMitigation, condition: condition, job: job}
}

// jobRefOf reads a Job's metric labels from its metadata.
func jobRefOf(job *batchv1.Job) jobRef {
	kind := jobKindMitigation
	if job.Labels[jobComponentLabel] == preMitigationJobComponent {
		kind = jobKindPreMitigation
	}
	return jobRef{kind: kind, condition: job.Labels[jobConditionLabel], job: job.Labels[jobNameLabel]}
}

func (j jobRef) count(nodeName, outcome string) {
	jobsTotal.WithLabelValues(nodeName, j.condition, j.kind, j.job, outcome).Inc()
}

func (j jobRef) setFailed(node *corev1.Node) {
	jobFailedInfo.WithLabelValues(node.Name, j.condition, j.kind, j.job).Set(1)
}

// clearFailed drops the job_failed_info series of the node's Job, whatever its condition.
func (j jobRef) clearFailed(node *corev1.Node) {
	jobFailedInfo.DeletePartialMatch(prometheus.Labels{labelNodeName: node.Name, labelJobKind: j.kind, labelJob: j.job})
}

// jobFinishedAt returns when the Job completed or failed, or the zero time
// while it runs.
func jobFinishedAt(job *batchv1.Job) time.Time {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return c.LastTransitionTime.Time
		}
	}
	return time.Time{}
}

// jobResult returns the outcome of a finished Job and a description of it, or
// "" while the Job runs.
func jobResult(job *batchv1.Job) (outcome, detail string) {
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			return jobOutcomeSucceeded, "succeeded"
		case batchv1.JobFailed:
			outcome, detail = jobOutcomeFailed, "failed: "+c.Reason
			if c.Reason == batchv1.JobReasonDeadlineExceeded {
				outcome, detail = jobOutcomeDeadline, "exceeded its deadline"
			}
			if c.Message != "" {
				detail += ": " + c.Message
			}
			return outcome, detail
		}
	}
	if !job.DeletionTimestamp.IsZero() {
		return jobOutcomeMissing, "was deleted before it finished"
	}
	return "", ""
}

func mitigationJobLaunchedAnnotation(job string) string {
	return annotationMitigationJobLaunchedPrefix + job
}

// mitigationJobLaunched reports whether the named mitigation Job launched for the node.
func mitigationJobLaunched(node *corev1.Node, job string) bool {
	return job != "" && node.Annotations[mitigationJobLaunchedAnnotation(job)] != ""
}

// launchMitigationJob launches the named Job once per node. It reports whether
// this call recorded the launch on the node. A Job missing from the
// configuration is reported once and not launched.
func (r *NodeReconciler) launchMitigationJob(ctx context.Context, node *corev1.Node, condition, name string, cfg Config) (bool, ctrl.Result) {
	if mitigationJobLaunched(node, name) {
		return false, ctrl.Result{}
	}
	log := logf.FromContext(ctx)
	ref := mitigationJobRef(condition, name)
	config := cfg.Jobs[name]
	if config == nil {
		// The plan outlived the Job's configuration. Adding it back launches it.
		if r.launchErrors.mark(node.UID, name) {
			err := fmt.Errorf("mitigation Job %q is not configured", name)
			log.Error(err, "Cannot launch mitigation Job, leaving the node as is", "node", node.Name, "condition", condition)
			ref.count(node.Name, jobOutcomeLaunchError)
			r.eventf(node, corev1.EventTypeWarning, eventReasonMitigationJobLaunchFailed, "MitigationJob",
				"Cannot launch mitigation Job for condition %s: %v", condition, err)
		}
		return false, ctrl.Result{}
	}
	// The cached node may predate the launch annotation of an earlier reconcile.
	if !r.launchingJobs.mark(node.UID, name) {
		return false, ctrl.Result{}
	}
	job := jobFromTemplate(r.Namespace, config.Template)
	prepareJob(job, node, mitigationJobComponent, name, condition, 1)
	injectJobEnvironment(job, node, condition, cfg.Conditions[condition].JobEnv)
	job, created, err := r.createOrAdoptJob(ctx, job)
	if err != nil {
		r.launchingJobs.unmark(node.UID, name)
		log.Error(err, "Failed to launch mitigation Job, will retry", "node", node.Name, "condition", condition, "job", name)
		if r.launchErrors.mark(node.UID, name) {
			ref.count(node.Name, jobOutcomeLaunchError)
		}
		reconcileErrorsTotal.WithLabelValues("launch_mitigation_job").Inc()
		r.eventf(node, corev1.EventTypeWarning, eventReasonMitigationJobLaunchFailed, "MitigationJob",
			"Failed to launch mitigation Job %q for condition %s; will retry: %v", name, condition, err)
		return false, ctrl.Result{RequeueAfter: drainRequeueInterval}
	}
	if created {
		ref.count(node.Name, jobOutcomeLaunched)
	}
	base := node.DeepCopy()
	node.Annotations[mitigationJobLaunchedAnnotation(name)] = job.Name
	if err := r.Patch(ctx, node, client.StrategicMergeFrom(base)); err != nil {
		r.launchingJobs.unmark(node.UID, name)
		log.Error(err, "Failed to record mitigation Job launch, will retry", "node", node.Name, "job", name)
		reconcileErrorsTotal.WithLabelValues("annotate_mitigation_job").Inc()
		return false, ctrl.Result{RequeueAfter: drainRequeueInterval}
	}
	r.observeMitigationDuration(node, condition)
	log.Info("Launched mitigation Job", "node", node.Name, "condition", condition, "job", name, "jobName", job.Name)
	r.eventf(node, corev1.EventTypeNormal, eventReasonMitigationJobLaunched, "MitigationJob",
		"Launched mitigation Job %q for condition %s: %s/%s", name, condition, job.Namespace, job.Name)
	return true, ctrl.Result{}
}

// launchedMitigationJobs returns the names of the node's launched mitigation Jobs.
func launchedMitigationJobs(node *corev1.Node) []string {
	var names []string
	for key := range node.Annotations {
		if name, ok := strings.CutPrefix(key, annotationMitigationJobLaunchedPrefix); ok {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// reportFailedMitigationJobs sets job_failed_info for each of the node's
// launched mitigation Jobs that failed, and clears it for the others. Deleting
// a failed Job clears it, so the delete acknowledges the failure.
func (r *NodeReconciler) reportFailedMitigationJobs(ctx context.Context, node *corev1.Node) ctrl.Result {
	for _, name := range launchedMitigationJobs(node) {
		jobName := node.Annotations[mitigationJobLaunchedAnnotation(name)]
		var job batchv1.Job
		err := r.jobReader().Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: jobName}, &job)
		if err != nil && !apierrors.IsNotFound(err) {
			logf.FromContext(ctx).Error(err, "Failed to read mitigation Job, will retry", "node", node.Name, "job", name, "jobName", jobName)
			reconcileErrorsTotal.WithLabelValues("get_mitigation_job").Inc()
			return ctrl.Result{RequeueAfter: drainRequeueInterval}
		}
		if outcome, _ := jobResult(&job); err == nil && job.DeletionTimestamp.IsZero() && (outcome == jobOutcomeFailed || outcome == jobOutcomeDeadline) {
			jobRefOf(&job).setFailed(node)
		} else {
			jobRef{kind: jobKindMitigation, job: name}.clearFailed(node)
		}
	}
	return ctrl.Result{}
}
