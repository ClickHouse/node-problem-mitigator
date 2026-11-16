package controller

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ = Describe("Job Controller", func() {
	const jobTemplates = `diagnostics:
  template:
    spec:
      template:
        spec:
          containers: [{name: diagnostics, image: registry.example/diagnostics:test}]
mitigate:
  template:
    spec:
      template:
        spec:
          containers: [{name: mitigate, image: registry.example/mitigate:test}]
`
	var nodes *NodeReconciler
	var jobs *JobReconciler

	configure := func(config string) {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ControllerConfigMap, Namespace: "default"}, Data: map[string]string{
			EnabledKey: "true", ConfigKey: config, JobsKey: jobTemplates,
		}}
		_ = k8sClient.Delete(ctx, cm)
		Expect(k8sClient.Create(ctx, cm)).To(Succeed())
	}
	// launch mitigates a new node and returns it with its only Job.
	launch := func(name, component string) (*corev1.Node, *batchv1.Job) {
		node := newTestNode(name, []corev1.NodeCondition{badCondition("CustomCheck")})
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, node) })
		_, err := nodes.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
		Expect(err).NotTo(HaveOccurred())
		list := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, list, client.InNamespace("default"),
			client.MatchingLabels{jobComponentLabel: component, jobTargetUIDLabel: string(node.UID)})).To(Succeed())
		Expect(list.Items).To(HaveLen(1))
		return node, &list.Items[0]
	}
	finish := func(job *batchv1.Job, outcome func(*batchv1.Job)) *batchv1.Job {
		running := job.DeepCopy()
		outcome(job)
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
		return running
	}
	reconcileJob := func(job *batchv1.Job) time.Duration {
		result, err := jobs.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: job.Namespace, Name: job.Name}})
		Expect(err).NotTo(HaveOccurred())
		return result.RequeueAfter
	}
	exists := func(job *batchv1.Job) bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Name}, &batchv1.Job{})
		if apierrors.IsNotFound(err) {
			return false
		}
		Expect(err).NotTo(HaveOccurred())
		return true
	}

	BeforeEach(func() {
		_ = k8sClient.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: StateConfigMap, Namespace: "default"}})
		configure("conditions:\n  CustomCheck: {drain: false, mitigationJob: {name: mitigate}}\njobRetention: '0'\ncooldown: '0s'\n")
		nodes = &NodeReconciler{Client: k8sClient, Timestamps: NewTimestampStore(k8sClient, "default"), Namespace: "default", APIReader: k8sClient, Recorder: events.NewFakeRecorder(64)}
		jobs = &JobReconciler{Client: k8sClient, Namespace: "default", Recorder: events.NewFakeRecorder(64)}
	})

	It("counts a mitigation Job that finishes after its node is deleted", func() {
		node, job := launch("job-outlives-node", mitigationJobComponent)
		before := counterValue(jobsTotal, node.Name, "CustomCheck", jobKindMitigation, "mitigate", jobOutcomeSucceeded)
		durations := histogramCount(jobDuration, jobKindMitigation, "mitigate")
		Expect(k8sClient.Delete(ctx, node)).To(Succeed())
		running := finish(job, succeedJob)

		jobs.outcomeRecorder().Update(event.UpdateEvent{ObjectOld: running, ObjectNew: job})
		Expect(counterValue(jobsTotal, node.Name, "CustomCheck", jobKindMitigation, "mitigate", jobOutcomeSucceeded)).To(Equal(before + 1))
		Expect(histogramCount(jobDuration, jobKindMitigation, "mitigate")).To(Equal(durations + 1))
		var recorded string
		Expect(jobs.Recorder.(*events.FakeRecorder).Events).To(Receive(&recorded))
		Expect(recorded).To(ContainSubstring(eventReasonMitigationJobSucceeded))
	})

	It("does not count a Job that was already finished when first seen", func() {
		node, job := launch("job-seen-finished", mitigationJobComponent)
		finish(job, failJob)
		before := counterValue(jobsTotal, node.Name, "CustomCheck", jobKindMitigation, "mitigate", jobOutcomeFailed)
		recorder := jobs.outcomeRecorder()
		recorder.Create(event.CreateEvent{Object: job})
		recorder.Update(event.UpdateEvent{ObjectOld: job.DeepCopy(), ObjectNew: job})
		recorder.Delete(event.DeleteEvent{Object: job})
		Expect(counterValue(jobsTotal, node.Name, "CustomCheck", jobKindMitigation, "mitigate", jobOutcomeFailed)).To(Equal(before))
	})

	It("counts a running Job deleted before it finished as missing", func() {
		node, job := launch("job-deleted-running", mitigationJobComponent)
		before := counterValue(jobsTotal, node.Name, "CustomCheck", jobKindMitigation, "mitigate", jobOutcomeMissing)
		jobs.outcomeRecorder().Delete(event.DeleteEvent{Object: job})
		Expect(counterValue(jobsTotal, node.Name, "CustomCheck", jobKindMitigation, "mitigate", jobOutcomeMissing)).To(Equal(before + 1))
	})

	It("keeps the failed Job of a live node and deletes it once the node is gone", func() {
		node, job := launch("job-failed-kept", mitigationJobComponent)
		finish(job, failJob)
		Expect(reconcileJob(job)).To(Equal(jobRecheckInterval))
		Expect(exists(job)).To(BeTrue())

		Expect(k8sClient.Delete(ctx, node)).To(Succeed())
		Expect(reconcileJob(job)).To(BeZero())
		Expect(exists(job)).To(BeFalse())
	})

	It("deletes a succeeded Job once jobRetention has passed", func() {
		configure("conditions:\n  CustomCheck: {drain: false, mitigationJob: {name: mitigate}}\njobRetention: 1h\ncooldown: '0s'\n")
		_, job := launch("job-retained", mitigationJobComponent)
		finish(job, succeedJob)
		Expect(reconcileJob(job)).To(BeNumerically("~", time.Hour, time.Minute))
		Expect(exists(job)).To(BeTrue())

		configure("conditions:\n  CustomCheck: {drain: false, mitigationJob: {name: mitigate}}\njobRetention: '0'\ncooldown: '0s'\n")
		Expect(reconcileJob(job)).To(BeZero())
		Expect(exists(job)).To(BeFalse())
	})

	It("keeps a succeeded pre-mitigation Job until the node records its outcome", func() {
		configure("conditions:\n  CustomCheck: {taint: false, preMitigationJob: {name: diagnostics}}\njobRetention: '0'\ncooldown: '0s'\n")
		node, job := launch("pre-job-unrecorded", preMitigationJobComponent)
		finish(job, succeedJob)
		Expect(reconcileJob(job)).To(Equal(jobRecheckInterval))
		Expect(exists(job)).To(BeTrue())

		_, err := nodes.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: node.Name}})
		Expect(err).NotTo(HaveOccurred())
		Expect(reconcileJob(job)).To(BeZero())
		Expect(exists(job)).To(BeFalse())
	})
})
