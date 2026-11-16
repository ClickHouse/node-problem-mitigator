package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8slabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// drainRequeueInterval is how often a drain in progress is retried. Each pass
	// re-lists the node's pods and re-issues evictions for any that remain,
	// letting pods blocked by a PodDisruptionBudget drain as budget frees up.
	drainRequeueInterval = 20 * time.Second

	// pdbBlockedRequeueInterval is the retry interval after an eviction pass had
	// an attempt blocked by a PodDisruptionBudget. Best-effort: an
	// event-driven reconcile (e.g. a node status update) can still run a pass
	// earlier.
	pdbBlockedRequeueInterval = 5 * time.Minute

	// Event reasons recorded on the Node during a drain, surfaced via
	// `kubectl describe node`.
	eventReasonEvictionBlocked = "EvictionBlocked"
	eventReasonEvictionFailed  = "EvictionFailed"
	// eventReasonDrainStarted marks the first pass of a drain.
	eventReasonDrainStarted = "DrainStarted"
	// eventReasonDrainTimeout marks a drain exceeding its timeout.
	eventReasonDrainTimeout = "DrainTimeout"
	// eventReasonDrainStalled marks a drain whose remaining pods are all
	// terminating past their deletion deadline with no kubelet removing them.
	eventReasonDrainStalled = "DrainStalled"
	// eventReasonDeleteFailed marks a failed direct deletion during that escalation.
	eventReasonDeleteFailed   = "DeleteFailed"
	eventReasonDrainCompleted = "DrainCompleted"

	// eventReasonEvicted is recorded on each pod the drain evicts.
	eventReasonEvicted = "Evicted"

	// eventReasonDrainRequested marks pods annotated for another controller to move.
	eventReasonDrainRequested     = "DrainRequested"
	eventReasonDrainRequestFailed = "DrainRequestFailed"

	// maxEventNoteLen is the API server's limit on an Event's message.
	maxEventNoteLen = 1024
	// maxEventPodListLen bounds the pod list embedded in a single drain Event;
	// longer lists are truncated at a pod boundary with a trailing "...". It
	// leaves headroom for the surrounding message text within maxEventNoteLen.
	maxEventPodListLen = 900
)

// reportDrainStalled records the DrainStalled Event for a node without a
// mitigation Job, naming the pods the kubelet is not removing. The pod names only feed the Event, so a
// failed list goes into the Event instead of a retry.
func (r *NodeReconciler) reportDrainStalled(ctx context.Context, node *corev1.Node, stallTimeout time.Duration, excludeNamespaces []string) {
	pods, err := r.evictablePods(ctx, node, excludeNamespaces)
	if err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list the pods of a stalled drain", "node", node.Name)
		reconcileErrorsTotal.WithLabelValues("list_pods_drain").Inc()
		r.eventf(node, corev1.EventTypeWarning, eventReasonDrainStalled, "Drain",
			"Drain stalled: every remaining pod has been terminating for over %s past its deletion deadline and the kubelet is not removing it; could not list them: %v",
			stallTimeout, err)
		return
	}
	r.eventf(node, corev1.EventTypeWarning, eventReasonDrainStalled, "Drain",
		"Drain stalled: %d pod(s) have been terminating for over %s past their deletion deadline and the kubelet is not removing them; the mitigation has no further step: %s",
		len(pods), stallTimeout, joinPodRefs(pods, maxEventPodListLen))
}

// drainOutcome is what a drain pass concluded about the node. Go has no enums,
// so it is a string-typed constant set.
type drainOutcome string

const (
	// drainInProgress: evictable pods remain and a later pass may still move
	// them (evictions pending, blocked by a PDB, or grace periods running).
	drainInProgress drainOutcome = "in-progress"
	// drainDone: no evictable pods remain and AnnotationDrainFinishedAt is set.
	drainDone drainOutcome = "done"
	// drainStalled: evictable pods remain, every one of them is terminating, and
	// the latest deletion deadline among them is more than stallTimeout in the past.
	drainStalled drainOutcome = "stalled"
	// drainSkipped: the drain timed out and skipPodDeletion leaves the remaining
	// pods in place.
	drainSkipped drainOutcome = "skipped"
)

// drainNode runs one pass of the drain. The first pass stamps
// AnnotationDrainStartedAt, so the timeout excludes time spent waiting for the
// pre-mitigation Job. Pods are evicted until timeout has elapsed, then deleted
// gracefully, bypassing PDBs; skipPodDeletion ends the drain at the timeout
// instead. A negative timeout evicts indefinitely.
func (r *NodeReconciler) drainNode(ctx context.Context, node *corev1.Node, condition string, timeout, stallTimeout time.Duration, skipPodDeletion bool, excludeNamespaces []string, reqs []DrainRequestConfig) (drainOutcome, ctrl.Result) {
	if _, started := node.Annotations[AnnotationDrainStartedAt]; !started {
		base := node.DeepCopy()
		if node.Annotations == nil {
			node.Annotations = make(map[string]string)
		}
		node.Annotations[AnnotationDrainStartedAt] = time.Now().UTC().Format(time.RFC3339)
		if err := r.Patch(ctx, node, client.StrategicMergeFrom(base)); err != nil {
			// Drop the unpersisted stamp: only a drain start persisted on the node can end the drain early.
			delete(node.Annotations, AnnotationDrainStartedAt)
			logf.FromContext(ctx).Error(err, "Failed to annotate drain start, will retry", "node", node.Name)
			reconcileErrorsTotal.WithLabelValues("annotate_drain_start").Inc()
			return drainInProgress, ctrl.Result{RequeueAfter: drainRequeueInterval}
		}
		timeoutDescription := "none"
		if timeout >= 0 {
			timeoutDescription = timeout.String()
		}
		r.eventf(node, corev1.EventTypeNormal, eventReasonDrainStarted, "Drain",
			"Started drain for condition %s; timeout %s", condition, timeoutDescription)
	}
	rem := removeEvict
	if drainTimedOut(node, timeout) {
		if skipPodDeletion {
			return drainSkipped, ctrl.Result{}
		}
		rem = removeDelete
	}
	return r.drainPass(ctx, node, condition, excludeNamespaces, rem, stallTimeout, reqs)
}

// drainPass runs a single eviction pass over the node's pods and reports whether
// the drain is complete. When no evictable pods remain it records completion
// exactly once by setting AnnotationDrainFinishedAt. It never observes the
// mitigation duration: the caller decides whether the drain is the last step.
// rem selects whether the pass evicts pods or deletes them directly (see
// podRemoval).
//
// Returns done=true only when the node is fully drained and the annotation is
// (already or newly) set. On a transient error it returns done=false with a
// requeue, mirroring the other cooldown-free retry paths.
func (r *NodeReconciler) drainPass(ctx context.Context, node *corev1.Node, condition string, excludeNamespaces []string, rem podRemoval, stallTimeout time.Duration, reqs []DrainRequestConfig) (drainOutcome, ctrl.Result) {
	log := logf.FromContext(ctx)

	state, err := r.removePods(ctx, node, condition, excludeNamespaces, rem, reqs)
	if err != nil {
		// Could not list the node's pods during drain. Counted separately from the
		// pre-taint exclusion-check list failure (reason=list_pods) so the two paths,
		// which have different requeue intervals and remediation, are distinguishable
		// on dashboards. Retry shortly.
		log.Error(err, "Failed to list pods during drain, will retry", "node", node.Name)
		reconcileErrorsTotal.WithLabelValues("list_pods_drain").Inc()
		return drainInProgress, ctrl.Result{RequeueAfter: drainRequeueInterval}
	}

	stalled := state.stalled(stallTimeout)
	setDrainStalledInfo(node.Name, condition, stalled)
	if stalled {
		log.Info("Drain stalled: every remaining pod is terminating past its deletion deadline and the kubelet is not removing it",
			"node", node.Name, "remaining", state.remaining, "lastDeadline", state.lastDeadline, "stallTimeout", stallTimeout)
		return drainStalled, ctrl.Result{RequeueAfter: drainRequeueInterval}
	}
	if state.remaining > 0 {
		if state.blocked > 0 {
			log.Info("Draining node, pods still remaining; evictions blocked by PodDisruptionBudget", "node", node.Name, "remaining", state.remaining, "blocked", state.blocked, "retryIn", pdbBlockedRequeueInterval)
			return drainInProgress, ctrl.Result{RequeueAfter: pdbBlockedRequeueInterval}
		}
		log.Info("Draining node, pods still remaining", "node", node.Name, "remaining", state.remaining)
		return drainInProgress, ctrl.Result{RequeueAfter: drainRequeueInterval}
	}

	// No evictable pods remain — the node is drained. Persist the annotation
	// before the caller observes any duration, so a failed patch cannot
	// double-count.
	base := node.DeepCopy()
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	node.Annotations[AnnotationDrainFinishedAt] = time.Now().UTC().Format(time.RFC3339)
	if err := r.Patch(ctx, node, client.StrategicMergeFrom(base)); err != nil {
		log.Error(err, "Failed to annotate node as drained, will retry", "node", node.Name)
		reconcileErrorsTotal.WithLabelValues("annotate_drained").Inc()
		return drainInProgress, ctrl.Result{RequeueAfter: drainRequeueInterval}
	}
	r.eventf(node, corev1.EventTypeNormal, eventReasonDrainCompleted, "Drain",
		"Completed drain for condition %s", condition)

	return drainDone, ctrl.Result{}
}

// setDrainStalledInfo sets or removes the node's drain_stalled_info series.
func setDrainStalledInfo(nodeName, condition string, stalled bool) {
	labels := prometheus.Labels{labelNodeName: nodeName, labelCondition: condition}
	if stalled {
		drainStalledInfo.With(labels).Set(1)
		return
	}
	drainStalledInfo.Delete(labels)
}

// podRemoval is how a single drain pass clears each pod. Go has no enums, so it
// is a string-typed constant set.
type podRemoval string

const (
	// removeEvict evicts via the Eviction API, honouring PodDisruptionBudgets.
	removeEvict podRemoval = "evict"
	// removeDelete deletes the pod directly (graceful), bypassing PDBs.
	removeDelete podRemoval = "delete"
)

// drainState is what a drain observed on the node's evictable pods.
type drainState struct {
	// remaining is the number of evictable pods still on the node.
	remaining int
	// blocked is how many of those had their eviction rejected by a
	// PodDisruptionBudget this pass.
	blocked int
	// lastDeadline is the latest deletionTimestamp among the remaining pods when
	// every one of them is already terminating; zero otherwise
	lastDeadline time.Time
}

// stalled reports whether every remaining pod has been past its deletion
// deadline for at least stallTimeout. A zero stallTimeout never stalls.
func (s drainState) stalled(stallTimeout time.Duration) bool {
	return stallTimeout > 0 && !s.lastDeadline.IsZero() && time.Since(s.lastDeadline) >= stallTimeout
}

// drainStateOf summarizes the evictable pods; blocked is left to the caller.
func drainStateOf(pods []corev1.Pod, excludeNamespaces []string) drainState {
	var st drainState
	allTerminating := true
	for i := range pods {
		pod := &pods[i]
		if !isEvictable(pod, excludeNamespaces) {
			continue
		}
		st.remaining++
		if pod.DeletionTimestamp == nil {
			allTerminating = false
			continue
		}
		if deadline := pod.DeletionTimestamp.Time; deadline.After(st.lastDeadline) {
			st.lastDeadline = deadline
		}
	}
	if !allTerminating {
		st.lastDeadline = time.Time{}
	}
	return st
}

// removePods makes a single pass at clearing the node's evictable pods and
// returns what it observed (see drainState).
//
// removeEvict evicts through the Eviction API, which honours
// PodDisruptionBudgets. removeDelete deletes directly, bypassing PDBs but
// honouring terminationGracePeriodSeconds.
//
// DaemonSet, mirror, completed, and excluded-namespace pods are skipped and
// never counted. Pods that are evicting or already terminating count toward
// remaining, so the drain is not complete until they leave the node.
func (r *NodeReconciler) removePods(ctx context.Context, node *corev1.Node, condition string, excludeNamespaces []string, rem podRemoval, reqs []DrainRequestConfig) (drainState, error) {
	log := logf.FromContext(ctx)

	var podList corev1.PodList
	if err := r.APIReader.List(ctx, &podList, client.MatchingFields{podNodeNameField: node.Name}); err != nil {
		return drainState{}, fmt.Errorf("listing pods on node %s: %w", node.Name, err)
	}

	st := drainStateOf(podList.Items, excludeNamespaces)
	var blocked, failedEvict, deleted, failedDelete, requested, failedRequest []string
	for i := range podList.Items {
		pod := &podList.Items[i]
		// Already terminating: wait for it. Never cut the grace period short.
		if pod.DeletionTimestamp != nil || !isEvictable(pod, excludeNamespaces) {
			continue
		}
		if rem == removeDelete {
			// Drain timed out: delete the remaining pod directly, bypassing PDBs.
			err := r.deletePod(ctx, pod)
			var outcome string
			switch {
			case err == nil:
				outcome = deleteOutcomeDeleted
				log.Info("Deleted pod after drain timeout", "node", node.Name, "pod", pod.Name, "namespace", pod.Namespace)
				deleted = append(deleted, pod.Namespace+"/"+pod.Name)
				r.Recorder.Eventf(pod, node, corev1.EventTypeWarning, eventReasonDrainTimeout, "Delete",
					"Deleted from node %s, bypassing PodDisruptionBudgets: the drain for condition %s exceeded its timeout",
					node.Name, condition)
			case apierrors.IsNotFound(err):
				// Pod disappeared between list and delete — it is already gone.
				outcome = deleteOutcomeDeleted
				st.remaining--
			default:
				outcome = deleteOutcomeError
				log.Error(err, "Failed to delete pod after drain timeout", "node", node.Name, "pod", pod.Name, "namespace", pod.Namespace)
				failedDelete = append(failedDelete, pod.Namespace+"/"+pod.Name)
			}
			drainPodActionsTotal.WithLabelValues(node.Name, condition, actionDeletePod, outcome).Inc()
			continue
		}

		if req := drainRequestFor(reqs, pod); req != nil {
			if !drainTimedOut(node, req.TimeoutDuration) {
				// Already requested, with any value: wait for the other controller to move the pod.
				if _, ok := pod.Annotations[req.AnnotationKey]; ok {
					continue
				}
				err := r.requestDrain(ctx, pod, req)
				switch {
				case err == nil:
					log.Info("Requested pod drain", "node", node.Name, "pod", pod.Name, "namespace", pod.Namespace)
					requested = append(requested, pod.Namespace+"/"+pod.Name)
					// annotationValue comes from config and has no length limit.
					r.Recorder.Eventf(pod, node, corev1.EventTypeNormal, eventReasonDrainRequested, "RequestDrain", "%s",
						eventNote("Annotated %s=%s to move off node %s while draining it for condition %s",
							req.AnnotationKey, req.AnnotationValue, node.Name, condition))
					drainPodActionsTotal.WithLabelValues(node.Name, condition, actionRequestDrain, requestOutcomeRequested).Inc()
				case apierrors.IsNotFound(err):
					st.remaining--
				default:
					log.Error(err, "Failed to request pod drain", "node", node.Name, "pod", pod.Name, "namespace", pod.Namespace)
					failedRequest = append(failedRequest, pod.Namespace+"/"+pod.Name)
					drainPodActionsTotal.WithLabelValues(node.Name, condition, actionRequestDrain, requestOutcomeError).Inc()
				}
				continue
			}
			log.Info("Drain request timed out, evicting pod", "node", node.Name, "pod", pod.Name, "namespace", pod.Namespace, "timeout", req.TimeoutDuration)
		}

		err := r.evictPod(ctx, pod)
		var outcome string
		switch {
		case err == nil:
			outcome = evictionOutcomeEvicted
			log.Info("Evicted pod", "node", node.Name, "pod", pod.Name, "namespace", pod.Namespace)
			r.Recorder.Eventf(pod, node, corev1.EventTypeNormal, eventReasonEvicted, "Evict",
				"Evicted from node %s while draining it for condition %s",
				node.Name, condition)
		case apierrors.IsNotFound(err):
			// Pod disappeared between list and evict — it is already gone.
			outcome = evictionOutcomeEvicted
			st.remaining--
		case apierrors.IsTooManyRequests(err):
			// Eviction would violate a PodDisruptionBudget; retry next pass.
			outcome = evictionOutcomeBlocked
			log.Info("Eviction blocked by PodDisruptionBudget, will retry", "node", node.Name, "pod", pod.Name, "namespace", pod.Namespace)
			blocked = append(blocked, pod.Namespace+"/"+pod.Name)
		default:
			outcome = evictionOutcomeError
			log.Error(err, "Failed to evict pod", "node", node.Name, "pod", pod.Name, "namespace", pod.Namespace)
			failedEvict = append(failedEvict, pod.Namespace+"/"+pod.Name)
		}
		drainPodActionsTotal.WithLabelValues(node.Name, condition, actionEvictPod, outcome).Inc()
	}

	// Emit at most one aggregated Event per outcome per pass.
	if len(requested) > 0 {
		r.eventf(node, corev1.EventTypeNormal, eventReasonDrainRequested, "RequestDrain",
			"Requested %d pod(s) to move off the node by annotation: %s",
			len(requested), joinPodRefs(requested, maxEventPodListLen))
	}
	if len(failedRequest) > 0 {
		r.eventf(node, corev1.EventTypeWarning, eventReasonDrainRequestFailed, "RequestDrain",
			"Failed to annotate %d pod(s) with a drain request: %s",
			len(failedRequest), joinPodRefs(failedRequest, maxEventPodListLen))
	}
	if len(blocked) > 0 {
		r.eventf(node, corev1.EventTypeWarning, eventReasonEvictionBlocked, "Evict",
			"Eviction blocked by a PodDisruptionBudget for %d pod(s); will retry: %s",
			len(blocked), joinPodRefs(blocked, maxEventPodListLen))
	}
	if len(failedEvict) > 0 {
		r.eventf(node, corev1.EventTypeWarning, eventReasonEvictionFailed, "Evict",
			"Failed to evict %d pod(s): %s",
			len(failedEvict), joinPodRefs(failedEvict, maxEventPodListLen))
	}
	if len(deleted) > 0 {
		r.eventf(node, corev1.EventTypeWarning, eventReasonDrainTimeout, "Delete",
			"Drain timeout exceeded; gracefully deleted %d pod(s), bypassing PodDisruptionBudgets: %s",
			len(deleted), joinPodRefs(deleted, maxEventPodListLen))
	}
	if len(failedDelete) > 0 {
		r.eventf(node, corev1.EventTypeWarning, eventReasonDeleteFailed, "Delete",
			"Failed to delete %d pod(s) after drain timeout: %s",
			len(failedDelete), joinPodRefs(failedDelete, maxEventPodListLen))
	}
	st.blocked = len(blocked)
	return st, nil
}

// evictablePods lists the "namespace/name" of every evictable pod still on the
// node, in API server order. The DrainTimeout and DrainStalled Events name them
// when a drain gives up and leaves them for the mitigation Job.
func (r *NodeReconciler) evictablePods(ctx context.Context, node *corev1.Node, excludeNamespaces []string) ([]string, error) {
	var podList corev1.PodList
	if err := r.APIReader.List(ctx, &podList, client.MatchingFields{podNodeNameField: node.Name}); err != nil {
		return nil, fmt.Errorf("listing pods on node %s: %w", node.Name, err)
	}
	var refs []string
	for i := range podList.Items {
		if pod := &podList.Items[i]; isEvictable(pod, excludeNamespaces) {
			refs = append(refs, pod.Namespace+"/"+pod.Name)
		}
	}
	return refs, nil
}

// eventf records an Event on the node.
func (r *NodeReconciler) eventf(node *corev1.Node, eventType, reason, action, format string, args ...any) {
	recordEvent(r.Recorder, node, eventType, reason, action, format, args...)
}

// recordEvent records an Event, truncating the message to maxEventNoteLen so
// the API server does not reject it.
func recordEvent(recorder events.EventRecorder, regarding runtime.Object, eventType, reason, action, format string, args ...any) {
	recorder.Eventf(regarding, nil, eventType, reason, action, "%s", eventNote(format, args...))
}

// eventNote formats an Event message and truncates it to maxEventNoteLen.
func eventNote(format string, args ...any) string {
	note := fmt.Sprintf(format, args...)
	if len(note) > maxEventNoteLen {
		note = note[:maxEventNoteLen-3] + "..."
	}
	return note
}

// joinPodRefs concatenates pod references with ", ", truncating to maxLen
// characters with a trailing "..." when the full list would overflow.
func joinPodRefs(refs []string, maxLen int) string {
	var b strings.Builder
	for i, ref := range refs {
		sep := ""
		if i > 0 {
			sep = ", "
		}
		if b.Len()+len(sep)+len(ref) > maxLen {
			if b.Len() > 0 {
				b.WriteString(", ")
			}
			b.WriteString("...")
			break
		}
		b.WriteString(sep)
		b.WriteString(ref)
	}
	return b.String()
}

// evictPod requests eviction of a single pod via the Eviction subresource. The
// API server enforces PodDisruptionBudgets, returning 429 (TooManyRequests)
// when a budget would be violated.
func (r *NodeReconciler) evictPod(ctx context.Context, pod *corev1.Pod) error {
	return r.SubResource("eviction").Create(ctx, pod, &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
	})
}

// deletePod deletes a pod directly, bypassing PodDisruptionBudgets but honouring
// its terminationGracePeriodSeconds.
func (r *NodeReconciler) deletePod(ctx context.Context, pod *corev1.Pod) error {
	return r.Delete(ctx, pod)
}

// drainRequestFor returns the first drain request whose selector matches the pod, or nil.
func drainRequestFor(reqs []DrainRequestConfig, pod *corev1.Pod) *DrainRequestConfig {
	for i := range reqs {
		if reqs[i].Selector.Matches(k8slabels.Set(pod.Labels)) {
			return &reqs[i]
		}
	}
	return nil
}

// requestDrain sets the drain request annotation on the pod with a JSON merge patch.
func (r *NodeReconciler) requestDrain(ctx context.Context, pod *corev1.Pod, req *DrainRequestConfig) error {
	base := pod.DeepCopy()
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[req.AnnotationKey] = req.AnnotationValue
	return r.Patch(ctx, pod, client.MergeFrom(base))
}

// isEvictable reports whether a pod should be evicted as part of draining a
// node. It mirrors the standard "kubectl drain" skip rules and additionally
// honours the controller's excluded namespaces so protected workloads are never
// evicted.
func isEvictable(pod *corev1.Pod, excludeNamespaces []string) bool {
	// Never evict workloads from protected namespaces.
	if slices.Contains(excludeNamespaces, pod.Namespace) {
		return false
	}
	// Completed pods are not running and need no eviction.
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return false
	}
	// Mirror (static) pods cannot be evicted via the API.
	if _, ok := pod.Annotations[corev1.MirrorPodAnnotationKey]; ok {
		return false
	}
	// DaemonSet pods tolerate the NoSchedule taint and are recreated immediately,
	// so evicting them is pointless; standard drain skips them.
	if owner := metav1.GetControllerOf(pod); owner != nil && owner.Kind == "DaemonSet" {
		return false
	}
	return true
}

// drainEnd names why a drain gave up on the pods still on the node: the Event
// reason and the clause that opens the Event naming them.
type drainEnd struct {
	reason  string
	summary string
}

// drainEndTimeout is a drain that ran past its drainTimeout with pod deletion skipped.
func drainEndTimeout() drainEnd {
	return drainEnd{reason: eventReasonDrainTimeout, summary: "Drain timeout exceeded"}
}

// drainEndStalled is a drain whose remaining pods all outlived their deletion
// deadline by more than stallTimeout with no kubelet removing them.
func drainEndStalled(stallTimeout time.Duration) drainEnd {
	return drainEnd{
		reason:  eventReasonDrainStalled,
		summary: fmt.Sprintf("Drain stalled, every remaining pod terminating for over %s past its deletion deadline", stallTimeout),
	}
}

// trackStallGauge keeps drain_stalled_info current after the mitigation Job
// launched, without touching the node's pods. It runs on reconciles that
// happen anyway (Node updates, the startup sync after a restart) and never
// requeues on its own.
func (r *NodeReconciler) trackStallGauge(ctx context.Context, node *corev1.Node, condition string, excludeNamespaces []string, stallTimeout time.Duration) {
	var podList corev1.PodList
	if err := r.APIReader.List(ctx, &podList, client.MatchingFields{podNodeNameField: node.Name}); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list pods for the drain stall gauge", "node", node.Name)
		reconcileErrorsTotal.WithLabelValues("list_pods_drain").Inc()
		return
	}
	setDrainStalledInfo(node.Name, condition, drainStateOf(podList.Items, excludeNamespaces).stalled(stallTimeout))
}

// drainTimedOut reports whether the drain ran past timeout, measured from
// AnnotationDrainStartedAt. A negative timeout or a missing/unparseable stamp
// returns false: the drain only ends early once the drain start is persisted
// on the node.
func drainTimedOut(node *corev1.Node, timeout time.Duration) bool {
	if timeout < 0 {
		return false
	}
	startedAt, err := time.Parse(time.RFC3339, node.Annotations[AnnotationDrainStartedAt])
	if err != nil {
		return false
	}
	return time.Since(startedAt) >= timeout
}
