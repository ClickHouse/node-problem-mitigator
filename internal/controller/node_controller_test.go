package controller

import (
	"context"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func newTestNode(name string, conditions []corev1.NodeCondition) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}, Status: corev1.NodeStatus{Conditions: conditions}}
}

func badCondition(condType corev1.NodeConditionType) corev1.NodeCondition {
	return corev1.NodeCondition{Type: condType, Status: corev1.ConditionTrue, Reason: "SomeNPDReason", LastTransitionTime: metav1.Now()}
}

func succeedJob(job *batchv1.Job) {
	start := job.CreationTimestamp
	completed := metav1.Now()
	job.Status.StartTime = &start
	job.Status.CompletionTime = &completed
	job.Status.Succeeded = 1
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
		{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: completed},
	}
}

func failJob(job *batchv1.Job) {
	start := job.CreationTimestamp
	failed := metav1.Now()
	job.Status.StartTime = &start
	job.Status.Failed = 1
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue},
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: failed},
	}
}

func counterValue(cv *prometheus.CounterVec, labels ...string) float64 {
	return testutil.ToFloat64(cv.WithLabelValues(labels...))
}

func histogramCount(hv *prometheus.HistogramVec, labels ...string) uint64 {
	observer, err := hv.GetMetricWithLabelValues(labels...)
	Expect(err).NotTo(HaveOccurred())
	var metric dto.Metric
	Expect(observer.(prometheus.Metric).Write(&metric)).To(Succeed())
	return metric.GetHistogram().GetSampleCount()
}

type oldTerminatingPodReader struct {
	client.Reader
}

func (r oldTerminatingPodReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := r.Reader.List(ctx, list, opts...); err != nil {
		return err
	}
	if pods, ok := list.(*corev1.PodList); ok {
		for i := range pods.Items {
			t := metav1.NewTime(time.Now().Add(-time.Minute))
			pods.Items[i].DeletionTimestamp = &t
		}
	}
	return nil
}

type failingPodReader struct {
	client.Reader
}

func (r failingPodReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.PodList); ok {
		return apierrors.NewServiceUnavailable("pod list unavailable")
	}
	return r.Reader.List(ctx, list, opts...)
}

type failingStateClient struct {
	client.Client
}

func (c failingStateClient) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	if key.Name == StateConfigMap {
		return apierrors.NewServiceUnavailable("state read unavailable")
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

type blockedEvictionClient struct {
	client.Client
}

func (c blockedEvictionClient) SubResource(name string) client.SubResourceClient {
	return blockedEvictionSubResource{SubResourceClient: c.Client.SubResource(name)}
}

type blockedEvictionSubResource struct {
	client.SubResourceClient
}

func (s blockedEvictionSubResource) Create(context.Context, client.Object, client.Object, ...client.SubResourceCreateOption) error {
	return apierrors.NewTooManyRequests("blocked by PodDisruptionBudget", 1)
}

type failingJobCreateClient struct {
	client.Client
}

func (c failingJobCreateClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*batchv1.Job); ok {
		return apierrors.NewServiceUnavailable("job creation unavailable")
	}
	return c.Client.Create(ctx, obj, opts...)
}

type failingPodPatchClient struct {
	client.Client
	err error
}

func (c failingPodPatchClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if _, ok := obj.(*corev1.Pod); ok {
		return c.err
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

// oldTerminatingNamedPodReader marks only the named pod as terminating.
type oldTerminatingNamedPodReader struct {
	client.Reader
	name string
}

func (r oldTerminatingNamedPodReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := r.Reader.List(ctx, list, opts...); err != nil {
		return err
	}
	if pods, ok := list.(*corev1.PodList); ok {
		for i := range pods.Items {
			if pods.Items[i].Name == r.name {
				t := metav1.NewTime(time.Now().Add(-time.Minute))
				pods.Items[i].DeletionTimestamp = &t
			}
		}
	}
	return nil
}

// staleNodeClient returns a fixed copy of one Node, like a cache that lags.
type staleNodeClient struct {
	client.Client
	node *corev1.Node
}

func (c staleNodeClient) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	if node, ok := obj.(*corev1.Node); ok && key.Name == c.node.Name {
		c.node.DeepCopyInto(node)
		return nil
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

var _ = Describe("Node Controller", func() {
	var reconciler *NodeReconciler
	var now time.Time

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

	configure := func(config string, jobs string) {
		cm := &corev1.ConfigMap{}
		key := types.NamespacedName{Name: ControllerConfigMap, Namespace: "default"}
		Expect(k8sClient.Get(ctx, key, cm)).To(Succeed())
		cm.Data[ConfigKey] = config
		if jobs != "" {
			cm.Data[JobsKey] = jobs
		} else {
			delete(cm.Data, JobsKey)
		}
		Expect(k8sClient.Update(ctx, cm)).To(Succeed())
	}
	newNode := func(name string, conditions ...corev1.NodeCondition) *corev1.Node {
		node := newTestNode(name, conditions)
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, node) })
		return node
	}
	reconcileNode := func(name string) (ctrl.Result, error) {
		return reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
	}
	getNode := func(name string) *corev1.Node {
		node := &corev1.Node{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, node)).To(Succeed())
		return node
	}
	jobsForNode := func(nodeName, component string) []batchv1.Job {
		list := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, list, client.InNamespace("default"), client.MatchingLabels{jobComponentLabel: component})).To(Succeed())
		var result []batchv1.Job
		for _, job := range list.Items {
			if job.Annotations[jobTargetNodeAnnotation] == nodeName {
				result = append(result, job)
			}
		}
		return result
	}
	flip := func(node *corev1.Node, condition string, status corev1.ConditionStatus) {
		oldNode := node.DeepCopy()
		for i := range node.Status.Conditions {
			if node.Status.Conditions[i].Type == corev1.NodeConditionType(condition) {
				node.Status.Conditions[i].Status = status
			}
		}
		reconciler.conditionFlipRecorder().Update(event.UpdateEvent{ObjectOld: oldNode, ObjectNew: node.DeepCopy()})
	}

	BeforeEach(func() {
		state := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: StateConfigMap, Namespace: "default"}}
		_ = k8sClient.Delete(ctx, state)
		config := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ControllerConfigMap, Namespace: "default"}, Data: map[string]string{
			EnabledKey: "true", ConfigKey: "conditions:\n  CustomCheck: {}\n  KernelDeadlock: {}\n",
		}}
		_ = k8sClient.Delete(ctx, config)
		Expect(k8sClient.Create(ctx, config)).To(Succeed())
		now = time.Now()
		timestamps := NewTimestampStore(k8sClient, "default")
		timestamps.now = func() time.Time { return now }
		reconciler = &NodeReconciler{Client: k8sClient, Timestamps: timestamps, Namespace: "default", APIReader: k8sClient, Recorder: events.NewFakeRecorder(64)}
	})

	It("does not act on a healthy or unconfigured condition", func() {
		node := newNode("healthy-node", corev1.NodeCondition{Type: corev1.NodeReady, Status: corev1.ConditionTrue})
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(getNode(node.Name).Annotations).NotTo(HaveKey(AnnotationMitigatedCondition))
	})

	It("runs taint, pre-mitigation Job, drain, then mitigation Job", func() {
		configure(`conditions:
  CustomCheck:
    preMitigationJob: {name: diagnostics, failurePolicy: Block}
    mitigationJob: {name: mitigate}
`, jobTemplates)
		node := newNode("ordered-pipeline", badCondition("CustomCheck"))
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched := getNode(node.Name)
		Expect(fetched.Annotations).NotTo(HaveKey(AnnotationDrainStartedAt))
		Expect(hasTaint(fetched)).To(BeTrue())
		Expect(fetched.Annotations[AnnotationMitigationPlan]).To(ContainSubstring("job=mitigate"))
		Expect(fetched.Annotations[AnnotationPreMitigationJobOutcome]).To(Equal(preMitigationJobRunning))
		preJobs := jobsForNode(node.Name, preMitigationJobComponent)
		Expect(preJobs).To(HaveLen(1))
		Expect(jobsForNode(node.Name, mitigationJobComponent)).To(BeEmpty())

		preJob := preJobs[0]
		succeedJob(&preJob)
		Expect(k8sClient.Status().Update(ctx, &preJob)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched = getNode(node.Name)
		Expect(fetched.Annotations[AnnotationPreMitigationJobOutcome]).To(Equal(jobOutcomeSucceeded))
		Expect(fetched.Annotations).To(HaveKey(AnnotationDrainFinishedAt))
		mitigationJobs := jobsForNode(node.Name, mitigationJobComponent)
		Expect(mitigationJobs).To(HaveLen(1))
		Expect(mitigationJobs[0].Name).To(Equal(fetched.Annotations[mitigationJobLaunchedAnnotation("mitigate")]))
	})

	It("blocks after a failed pre-mitigation Job until the operator continues", func() {
		configure("conditions:\n  CustomCheck: {preMitigationJob: {name: diagnostics, failurePolicy: Block}, mitigationJob: {name: mitigate}}\n", jobTemplates)
		node := newNode("blocked-pipeline", badCondition("CustomCheck"))
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		job := jobsForNode(node.Name, preMitigationJobComponent)[0]
		failJob(&job)
		Expect(k8sClient.Status().Update(ctx, &job)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched := getNode(node.Name)
		Expect(fetched.Annotations[AnnotationPreMitigationJobOutcome]).To(Equal(jobOutcomeFailed))
		Expect(fetched.Annotations).NotTo(HaveKey(AnnotationDrainFinishedAt))
		Expect(jobsForNode(node.Name, mitigationJobComponent)).To(BeEmpty())

		fetched.Annotations[AnnotationPreMitigationJobAction] = PreMitigationJobActionContinue
		Expect(k8sClient.Update(ctx, fetched)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(jobsForNode(node.Name, mitigationJobComponent)).To(HaveLen(1))
	})

	It("does not rerun a failed pre-mitigation Job after retry action starts a new attempt", func() {
		configure("conditions:\n  CustomCheck: {preMitigationJob: {name: diagnostics}}\n", jobTemplates)
		node := newNode("retry-pipeline", badCondition("CustomCheck"))
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		first := jobsForNode(node.Name, preMitigationJobComponent)[0]
		failJob(&first)
		Expect(k8sClient.Status().Update(ctx, &first)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched := getNode(node.Name)
		fetched.Annotations[AnnotationPreMitigationJobAction] = PreMitigationJobActionRetry
		Expect(k8sClient.Update(ctx, fetched)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched = getNode(node.Name)
		Expect(fetched.Annotations[AnnotationPreMitigationJobAttempt]).To(Equal("2"))
		Expect(jobsForNode(node.Name, preMitigationJobComponent)).To(HaveLen(2))
	})

	It("reports a failed mitigation Job until the Job is deleted", func() {
		configure("conditions:\n  CustomCheck: {drain: false, mitigationJob: {name: mitigate}}\n", jobTemplates)
		node := newNode("job-outcomes", badCondition("CustomCheck"))
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		job := jobsForNode(node.Name, mitigationJobComponent)[0]
		Expect(counterValue(jobsTotal, node.Name, "CustomCheck", jobKindMitigation, "mitigate", jobOutcomeLaunched)).To(Equal(1.0))
		failJob(&job)
		Expect(k8sClient.Status().Update(ctx, &job)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(testutil.ToFloat64(jobFailedInfo.WithLabelValues(node.Name, "CustomCheck", jobKindMitigation, "mitigate"))).To(Equal(1.0))

		Expect(k8sClient.Delete(ctx, &job)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(jobFailedInfo.DeleteLabelValues(node.Name, "CustomCheck", jobKindMitigation, "mitigate")).To(BeFalse())
	})

	It("reports a missing mitigation Job template once", func() {
		configure("conditions:\n  CustomCheck: {preMitigationJob: {name: diagnostics}, mitigationJob: {name: mitigate}}\n", jobTemplates)
		node := newNode("missing-template", badCondition("CustomCheck"))
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		preJob := jobsForNode(node.Name, preMitigationJobComponent)[0]

		configure("conditions:\n  CustomCheck: {preMitigationJob: {name: diagnostics}}\n", "diagnostics:\n  template:\n    spec:\n      template:\n        spec:\n          containers: [{name: diagnostics, image: registry.example/diagnostics:test}]\n")
		succeedJob(&preJob)
		Expect(k8sClient.Status().Update(ctx, &preJob)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(counterValue(jobsTotal, node.Name, "CustomCheck", jobKindMitigation, "mitigate", jobOutcomeLaunchError)).To(Equal(1.0))
	})

	It("adopts a pre-mitigation Job after losing its outcome annotation", func() {
		configure("conditions:\n  CustomCheck: {preMitigationJob: {name: diagnostics}}\n", jobTemplates)
		node := newNode("adopt-pre-job", badCondition("CustomCheck"))
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		jobs := jobsForNode(node.Name, preMitigationJobComponent)
		Expect(jobs).To(HaveLen(1))
		jobName := jobs[0].Name

		fetched := getNode(node.Name)
		delete(fetched.Annotations, AnnotationPreMitigationJobOutcome)
		Expect(k8sClient.Update(ctx, fetched)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched = getNode(node.Name)
		Expect(fetched.Annotations[AnnotationPreMitigationJobOutcome]).To(Equal(preMitigationJobRunning))
		Expect(jobsForNode(node.Name, preMitigationJobComponent)).To(HaveLen(1))
		Expect(jobsForNode(node.Name, preMitigationJobComponent)[0].Name).To(Equal(jobName))
	})

	It("retries mitigation Job creation after a launch error, counting the error once", func() {
		configure("conditions:\n  CustomCheck: {drain: false, mitigationJob: {name: mitigate}}\n", jobTemplates)
		node := newNode("job-create-retry", badCondition("CustomCheck"))
		reconciler.Client = failingJobCreateClient{Client: k8sClient}
		for range 2 {
			_, err := reconcileNode(node.Name)
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(counterValue(jobsTotal, node.Name, "CustomCheck", jobKindMitigation, "mitigate", jobOutcomeLaunchError)).To(Equal(1.0))
		reconciler.Client = k8sClient
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(jobsForNode(node.Name, mitigationJobComponent)).To(HaveLen(1))
		Expect(counterValue(jobsTotal, node.Name, "CustomCheck", jobKindMitigation, "mitigate", jobOutcomeLaunched)).To(Equal(1.0))
	})

	It("ends an observe-only mitigation after recovery and starts a new episode with a new Job name", func() {
		configure("conditions:\n  CustomCheck: {taint: false, preMitigationJob: {name: diagnostics}}\n", jobTemplates)
		node := newNode("observe-only", badCondition("CustomCheck"))
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		first := jobsForNode(node.Name, preMitigationJobComponent)[0]
		succeedJob(&first)
		Expect(k8sClient.Status().Update(ctx, &first)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched := getNode(node.Name)
		flip(fetched, "CustomCheck", corev1.ConditionFalse)
		Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched = getNode(node.Name)
		Expect(fetched.Annotations).NotTo(HaveKey(AnnotationMitigatedCondition))
		Expect(fetched.Annotations).NotTo(HaveKey(AnnotationMitigationPlan))
		Expect(fetched.Annotations).NotTo(HaveKey(AnnotationPreMitigationJob))

		time.Sleep(time.Second)
		Expect(fetched.Annotations).NotTo(HaveKey(AnnotationMitigatedAt))
		flip(fetched, "CustomCheck", corev1.ConditionTrue)
		Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		allJobs := jobsForNode(node.Name, preMitigationJobComponent)
		Expect(allJobs).To(HaveLen(2))
		Expect(allJobs[1].Name).NotTo(Equal(allJobs[0].Name))
	})

	It("starts an operator-selected Manual mitigation only when Manual is configured", func() {
		node := newNode("manual-without-config")
		node.Annotations = map[string]string{AnnotationMitigatedCondition: "Manual"}
		Expect(k8sClient.Update(ctx, node)).To(Succeed())
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(getNode(node.Name).Annotations).NotTo(HaveKey(AnnotationMitigationPlan))

		configure("conditions:\n  CustomCheck: {}\n  Manual: {drain: false, mitigationJob: {name: mitigate}}\n", jobTemplates)
		node = getNode(node.Name)
		node.Annotations[AnnotationMitigatedCondition] = "Manual"
		Expect(k8sClient.Update(ctx, node)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(jobsForNode(node.Name, mitigationJobComponent)).To(HaveLen(1))
	})

	It("takes over only when the merged plan changes", func() {
		configure("conditions:\n  CustomCheck: {taint: false, drain: false}\n  KernelDeadlock: {}\n", "")
		node := newNode("takeover-node", badCondition("CustomCheck"))
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched := getNode(node.Name)
		fetched.Status.Conditions = append(fetched.Status.Conditions, badCondition("KernelDeadlock"))
		Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched = getNode(node.Name)
		Expect(fetched.Annotations[AnnotationMitigatedCondition]).To(Equal("KernelDeadlock"))
		p, err := parsePlan(fetched.Annotations[AnnotationMitigationPlan])
		Expect(err).NotTo(HaveOccurred())
		Expect(p.taint).To(BeTrue())
	})

	It("pauses a committed mitigation when the controller is disabled", func() {
		configure("conditions:\n  CustomCheck: {preMitigationJob: {name: diagnostics}}\n", jobTemplates)
		node := newNode("disabled-controller", badCondition("CustomCheck"))
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched := getNode(node.Name)
		Expect(fetched.Annotations[AnnotationPreMitigationJobOutcome]).To(Equal(preMitigationJobRunning))
		Expect(fetched.Annotations).NotTo(HaveKey(AnnotationDrainStartedAt))
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ControllerConfigMap, Namespace: "default"}, cm)).To(Succeed())
		cm.Data[EnabledKey] = "false"
		Expect(k8sClient.Update(ctx, cm)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(hasTaint(getNode(node.Name))).To(BeTrue())
	})

	It("rolls back the cooldown timestamp when persistence fails", func() {
		store := NewTimestampStore(failingStateClient{Client: k8sClient}, "default")
		store.now = func() time.Time { return now }
		Expect(store.Record(ctx)).To(HaveOccurred())
		Expect(store.CanOperate(ctx, time.Hour)).To(BeTrue())
	})

	It("skips nodes excluded by annotation, taint, or namespace and fails closed on pod-list errors", func() {
		configure(`conditions:
  CustomCheck: {}
excludeNodeAnnotations: {maintenance: "true"}
excludeNodeTaints:
  - key: example.com/disrupted
excludeNamespaces: [protected]
`, "")
		annotated := newNode("excluded-annotation", badCondition("CustomCheck"))
		annotated.Annotations = map[string]string{"maintenance": "true"}
		Expect(k8sClient.Update(ctx, annotated)).To(Succeed())
		_, err := reconcileNode(annotated.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(getNode(annotated.Name).Annotations).NotTo(HaveKey(AnnotationMitigationPlan))

		tainted := newNode("excluded-taint", badCondition("CustomCheck"))
		tainted.Spec.Taints = []corev1.Taint{{Key: "example.com/disrupted", Effect: corev1.TaintEffectNoSchedule}}
		Expect(k8sClient.Update(ctx, tainted)).To(Succeed())
		_, err = reconcileNode(tainted.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(getNode(tainted.Name).Annotations).NotTo(HaveKey(AnnotationMitigationPlan))

		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "protected"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, ns) })
		protected := newNode("excluded-namespace", badCondition("CustomCheck"))
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "protected-workload", Namespace: ns.Name}, Spec: corev1.PodSpec{NodeName: protected.Name, Containers: []corev1.Container{{Name: "c", Image: "busybox"}}}}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod) })
		_, err = reconcileNode(protected.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(getNode(protected.Name).Annotations).NotTo(HaveKey(AnnotationMitigationPlan))

		failedCheck := newNode("exclude-check-failed", badCondition("CustomCheck"))
		reconciler.APIReader = failingPodReader{Reader: k8sClient}
		result, err := reconcileNode(failedCheck.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(30 * time.Second))
		Expect(getNode(failedCheck.Name).Annotations).NotTo(HaveKey(AnnotationMitigationPlan))
	})

	It("counts one pending skip across a for window and starts after it elapses", func() {
		configure("conditions:\n  CustomCheck: {for: 5m}\n", "")
		pending := badCondition("CustomCheck")
		pending.Status = corev1.ConditionFalse
		pending.LastTransitionTime = metav1.NewTime(time.Now().Add(-time.Minute))
		node := newNode("pending-window", pending)
		node = getNode(node.Name)
		flip(node, "CustomCheck", corev1.ConditionTrue)
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
		before := counterValue(mitigationSkippedTotal, "CustomCheck", skipReasonPending)
		result, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeNumerically("~", 4*time.Minute, time.Second))
		Expect(hasTaint(getNode(node.Name))).To(BeFalse())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(counterValue(mitigationSkippedTotal, "CustomCheck", skipReasonPending)).To(Equal(before + 1))

		node = getNode(node.Name)
		node.Status.Conditions[0].LastTransitionTime = metav1.NewTime(time.Now().Add(-6 * time.Minute))
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(hasTaint(getNode(node.Name))).To(BeTrue())
	})

	It("gates a takeover that turns on taint through the cooldown", func() {
		configure("conditions:\n  CustomCheck: {taint: false, drain: false}\n  KernelDeadlock: {}\ncooldown: 1h\n", "")
		Expect(reconciler.Timestamps.Record(ctx)).To(Succeed())
		node := newNode("cooldown-takeover", badCondition("CustomCheck"))
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched := getNode(node.Name)
		fetched.Status.Conditions = append(fetched.Status.Conditions, badCondition("KernelDeadlock"))
		Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched = getNode(node.Name)
		Expect(fetched.Annotations[AnnotationMitigatedCondition]).To(Equal("CustomCheck"))
		Expect(hasTaint(fetched)).To(BeFalse())

		reconciler.Timestamps.now = func() time.Time { return now.Add(2 * time.Hour) }
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched = getNode(node.Name)
		Expect(fetched.Annotations[AnnotationMitigatedCondition]).To(Equal("KernelDeadlock"))
		Expect(hasTaint(fetched)).To(BeTrue())
		recorder := reconciler.Recorder.(*events.FakeRecorder)
		var first, escalation string
		Expect(recorder.Events).To(Receive(&first))
		Expect(recorder.Events).To(Receive(&escalation))
		Expect(escalation).To(ContainSubstring(eventReasonMitigationEscalated))
	})

	It("keeps deleted-node metrics through the scrape grace period then removes them", func() {
		before := testutil.CollectAndCount(jobsTotal)
		jobsTotal.WithLabelValues("metrics-cleanup-node", "CustomCheck", jobKindMitigation, "mitigate", jobOutcomeSucceeded).Inc()
		Expect(testutil.CollectAndCount(jobsTotal)).To(Equal(before + 1))
		Expect(reconciler.scheduleNodeMetricsCleanup("metrics-cleanup-node")).To(Equal(metricsCleanupGracePeriod))
		reconciler.pendingMetricCleanupsMu.Lock()
		reconciler.pendingMetricCleanups["metrics-cleanup-node"] = time.Now().Add(-metricsCleanupGracePeriod - time.Second)
		reconciler.pendingMetricCleanupsMu.Unlock()
		Expect(reconciler.scheduleNodeMetricsCleanup("metrics-cleanup-node")).To(BeZero())
		Expect(testutil.CollectAndCount(jobsTotal)).To(Equal(before))
	})

	It("deletes pods gracefully after a drain timeout", func() {
		configure("conditions:\n  CustomCheck: {drainTimeout: 1s}\n", "")
		node := newNode("drain-timeout-delete", badCondition("CustomCheck"))
		node.Annotations = map[string]string{
			AnnotationMitigatedCondition: "CustomCheck",
			AnnotationMitigatedAt:        now.Add(-time.Hour).Format(time.RFC3339Nano),
			AnnotationMitigationPlan:     (plan{taint: true, drain: true, drainTimeout: time.Second}).String(),
			AnnotationDrainStartedAt:     now.Add(-time.Minute).Format(time.RFC3339Nano),
		}
		Expect(k8sClient.Update(ctx, node)).To(Succeed())
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "timeout-delete", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: node.Name, Containers: []corev1.Container{{Name: "c", Image: "busybox"}}}}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod) })
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		deleted := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, deleted)).To(Succeed())
		Expect(deleted.DeletionTimestamp).NotTo(BeNil())
		Expect(counterValue(drainPodActionsTotal, node.Name, "CustomCheck", actionDeletePod, deleteOutcomeDeleted)).To(Equal(1.0))
	})

	It("leaves pods after timeout when skipPodDeletion is enabled", func() {
		configure("conditions:\n  CustomCheck: {drainTimeout: 1s, skipPodDeletion: true, mitigationJob: {name: mitigate}}\n", jobTemplates)
		node := newNode("drain-timeout-skip", badCondition("CustomCheck"))
		node.Annotations = map[string]string{
			AnnotationMitigatedCondition: "CustomCheck",
			AnnotationMitigatedAt:        now.Add(-time.Hour).Format(time.RFC3339Nano),
			AnnotationMitigationPlan:     (plan{taint: true, drain: true, drainTimeout: time.Second, skipPodDeletion: true, job: "mitigate"}).String(),
			AnnotationDrainStartedAt:     now.Add(-time.Minute).Format(time.RFC3339Nano),
		}
		Expect(k8sClient.Update(ctx, node)).To(Succeed())
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "timeout-skip", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: node.Name, Containers: []corev1.Container{{Name: "c", Image: "busybox"}}}}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod) })
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, &corev1.Pod{})).To(Succeed())
		Expect(jobsForNode(node.Name, mitigationJobComponent)).To(HaveLen(1))
	})

	It("records the recovery duration of a watched condition", func() {
		configure("conditions:\n  CustomCheck: {taint: false, drain: false}\n", "")
		node := newNode("recovery-metric", badCondition("CustomCheck"))
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched := getNode(node.Name)
		before := histogramCount(conditionRecoveryTime, "CustomCheck")
		fetched.Status.Conditions[0].LastTransitionTime = metav1.NewTime(time.Now().Add(-time.Minute))
		Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
		oldNode := fetched.DeepCopy()
		fetched.Status.Conditions[0].Status = corev1.ConditionFalse
		fetched.Status.Conditions[0].LastTransitionTime = metav1.NewTime(time.Now())
		reconciler.conditionFlipRecorder().Update(event.UpdateEvent{ObjectOld: oldNode, ObjectNew: fetched.DeepCopy()})
		Expect(histogramCount(conditionRecoveryTime, "CustomCheck")).To(Equal(before + 1))
	})

	It("retries an eviction blocked by a PodDisruptionBudget", func() {
		configure("conditions:\n  CustomCheck: {}\n", "")
		node := newNode("pdb-blocked", badCondition("CustomCheck"))
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "protected", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: node.Name, Containers: []corev1.Container{{Name: "c", Image: "busybox"}}}}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod) })
		reconciler.Client = blockedEvictionClient{Client: k8sClient}
		result, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(pdbBlockedRequeueInterval))
		Expect(counterValue(drainPodActionsTotal, node.Name, "CustomCheck", actionEvictPod, evictionOutcomeBlocked)).To(Equal(1.0))
	})

	It("skips a node excluded by label and does not taint in dry-run mode", func() {
		configure("conditions:\n  CustomCheck: {}\nexcludeNodeLabels: {maintenance: 'true'}\n", "")
		excluded := newNode("excluded-label", badCondition("CustomCheck"))
		excluded.Labels = map[string]string{"maintenance": "true"}
		Expect(k8sClient.Update(ctx, excluded)).To(Succeed())
		_, err := reconcileNode(excluded.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(getNode(excluded.Name).Annotations).NotTo(HaveKey(AnnotationMitigationPlan))

		configure("conditions:\n  CustomCheck: {}\n", "")
		reconciler.DryRun = true
		dryRun := newNode("dry-run", badCondition("CustomCheck"))
		_, err = reconcileNode(dryRun.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(hasTaint(getNode(dryRun.Name))).To(BeFalse())
		Expect(testutil.ToFloat64(nodeMitigatedInfo.WithLabelValues(dryRun.Name, "CustomCheck", "false"))).To(Equal(1.0))
	})

	It("injects jobEnv and adopts the deterministic Job after losing its node annotation", func() {
		configure("conditions:\n  CustomCheck: {drain: false, jobEnv: {CUSTOM_SETTING: configured}, mitigationJob: {name: mitigate}}\n", jobTemplates)
		node := newNode("adopt-job", badCondition("CustomCheck"))
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		jobs := jobsForNode(node.Name, mitigationJobComponent)
		Expect(jobs).To(HaveLen(1))
		jobName := jobs[0].Name
		var foundEnv bool
		for _, env := range jobs[0].Spec.Template.Spec.Containers[0].Env {
			if env.Name == "CUSTOM_SETTING" && env.Value == "configured" {
				foundEnv = true
			}
		}
		Expect(foundEnv).To(BeTrue())

		fetched := getNode(node.Name)
		delete(fetched.Annotations, mitigationJobLaunchedAnnotation("mitigate"))
		Expect(k8sClient.Update(ctx, fetched)).To(Succeed())
		reconciler = &NodeReconciler{Client: k8sClient, Timestamps: reconciler.Timestamps, Namespace: "default", APIReader: k8sClient, Recorder: events.NewFakeRecorder(64)}
		_, err = reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched = getNode(node.Name)
		Expect(fetched.Annotations[mitigationJobLaunchedAnnotation("mitigate")]).To(Equal(jobName))
		Expect(jobsForNode(node.Name, mitigationJobComponent)).To(HaveLen(1))
	})

	It("does not launch a Job when a drain stalls without a mitigation Job", func() {
		configure("conditions:\n  CustomCheck: {}\nstallTimeout: 1s\n", "")
		node := newNode("stalled-drain", badCondition("CustomCheck"))
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "stuck-no-job", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: node.Name, Containers: []corev1.Container{{Name: "c", Image: "busybox"}}}}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod) })
		reconciler.APIReader = oldTerminatingPodReader{Reader: k8sClient}
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(jobsForNode(node.Name, mitigationJobComponent)).To(BeEmpty())
		Expect(testutil.ToFloat64(drainStalledInfo.WithLabelValues(node.Name, "CustomCheck"))).To(Equal(1.0))
	})

	It("moves a stalled drain to the mitigation Job", func() {
		configure("conditions:\n  CustomCheck: {mitigationJob: {name: mitigate}}\nstallTimeout: 1s\n", jobTemplates)
		node := newNode("stalled-with-job", badCondition("CustomCheck"))
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "stuck-with-job", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: node.Name, Containers: []corev1.Container{{Name: "c", Image: "busybox"}}}}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod) })
		reconciler.APIReader = oldTerminatingPodReader{Reader: k8sClient}
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(getNode(node.Name).Annotations).NotTo(HaveKey(AnnotationDrainFinishedAt))
		Expect(jobsForNode(node.Name, mitigationJobComponent)).To(HaveLen(1))
	})

	It("does not start a second mitigation from a cached node that predates the commit", func() {
		configure("conditions:\n  CustomCheck: {taint: false, preMitigationJob: {name: diagnostics}}\n", jobTemplates)
		node := newNode("stale-cache", badCondition("CustomCheck"))
		stale := getNode(node.Name)
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		started := counterValue(nodeMitigatedTotal, "CustomCheck", "false")

		reconciler.Client = staleNodeClient{Client: k8sClient, node: stale}
		result, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(time.Second))
		Expect(counterValue(nodeMitigatedTotal, "CustomCheck", "false")).To(Equal(started))
		Expect(jobsForNode(node.Name, preMitigationJobComponent)).To(HaveLen(1))
	})

	It("keeps an earlier condition's pre-mitigation Job when a later condition takes over", func() {
		configure(`conditions:
  CustomCheck:
    preMitigationJob: {name: diagnostics, failurePolicy: Block}
  KernelDeadlock:
    mitigationJob: {name: mitigate}
`, jobTemplates)
		node := newNode("takeover-pre-job", badCondition("CustomCheck"), badCondition("KernelDeadlock"))
		_, err := reconcileNode(node.Name)
		Expect(err).NotTo(HaveOccurred())
		fetched := getNode(node.Name)
		Expect(fetched.Annotations[AnnotationMitigatedCondition]).To(Equal("KernelDeadlock"))
		Expect(fetched.Annotations[AnnotationPreMitigationJob]).To(Equal("diagnostics"))
		Expect(fetched.Annotations[AnnotationPreMitigationJobCondition]).To(Equal("CustomCheck"))
		Expect(jobsForNode(node.Name, preMitigationJobComponent)).To(HaveLen(1))
		Expect(jobsForNode(node.Name, mitigationJobComponent)).To(BeEmpty())
	})

	Context("drain request", func() {
		const requestKey = "example.com/drain-requested"
		requestConfig := func(condition, timeout string) string {
			return "conditions:\n  CustomCheck: " + condition + "\n" +
				"drainRequests: [{podSelector: {matchLabels: {role: db}}, annotationKey: " + requestKey + timeout + "}]\n"
		}
		newPod := func(name, nodeName string, labels map[string]string) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels}, Spec: corev1.PodSpec{NodeName: nodeName, Containers: []corev1.Container{{Name: "c", Image: "busybox"}}}}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod) })
		}
		getPod := func(name string) *corev1.Pod {
			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, pod)).To(Succeed())
			return pod
		}
		// nodeEvents drains the recorder and keeps the node-level Events with the given prefix.
		nodeEvents := func(prefix string) []string {
			recorder := reconciler.Recorder.(*events.FakeRecorder)
			var matched []string
			for {
				select {
				case e := <-recorder.Events:
					if strings.HasPrefix(e, prefix) {
						matched = append(matched, e)
					}
				default:
					return matched
				}
			}
		}
		startedNode := func(name string, drainTimeout, startedAgo time.Duration) *corev1.Node {
			node := newNode(name, badCondition("CustomCheck"))
			node.Annotations = map[string]string{
				AnnotationMitigatedCondition: "CustomCheck",
				AnnotationMitigatedAt:        now.Add(-time.Hour).Format(time.RFC3339Nano),
				AnnotationMitigationPlan:     (plan{taint: true, drain: true, drainTimeout: drainTimeout}).String(),
				AnnotationDrainStartedAt:     now.Add(-startedAgo).Format(time.RFC3339Nano),
			}
			Expect(k8sClient.Update(ctx, node)).To(Succeed())
			return node
		}

		It("annotates a matching pod instead of evicting it and evicts the others", func() {
			configure(requestConfig("{}", ""), "")
			node := newNode("request-annotates", badCondition("CustomCheck"))
			newPod("request-db", node.Name, map[string]string{"role": "db"})
			newPod("request-web", node.Name, map[string]string{"role": "web"})
			_, err := reconcileNode(node.Name)
			Expect(err).NotTo(HaveOccurred())
			db := getPod("request-db")
			Expect(db.Annotations).To(HaveKeyWithValue(requestKey, "true"))
			Expect(db.DeletionTimestamp).To(BeNil())
			Expect(getPod("request-web").DeletionTimestamp).NotTo(BeNil())
			Expect(getNode(node.Name).Annotations).NotTo(HaveKey(AnnotationDrainFinishedAt))
			Expect(counterValue(drainPodActionsTotal, node.Name, "CustomCheck", actionRequestDrain, requestOutcomeRequested)).To(Equal(1.0))
			Expect(counterValue(drainPodActionsTotal, node.Name, "CustomCheck", actionEvictPod, evictionOutcomeEvicted)).To(Equal(1.0))
		})

		It("truncates the pod Event when annotationValue is long", func() {
			configure("conditions:\n  CustomCheck: {}\n"+
				"drainRequests: [{podSelector: {matchLabels: {role: db}}, annotationKey: "+requestKey+", annotationValue: "+strings.Repeat("v", 2*maxEventNoteLen)+"}]\n", "")
			node := newNode("request-long-value", badCondition("CustomCheck"))
			newPod("request-long-db", node.Name, map[string]string{"role": "db"})
			_, err := reconcileNode(node.Name)
			Expect(err).NotTo(HaveOccurred())
			podEvents := nodeEvents("Normal DrainRequested Annotated ")
			Expect(podEvents).To(HaveLen(1))
			note := strings.TrimPrefix(podEvents[0], "Normal DrainRequested ")
			Expect(note).To(HaveLen(maxEventNoteLen))
			Expect(note).To(HaveSuffix("..."))
		})

		It("does not patch an already annotated pod again", func() {
			configure(requestConfig("{}", ""), "")
			node := newNode("request-once", badCondition("CustomCheck"))
			newPod("request-once-db", node.Name, map[string]string{"role": "db"})
			_, err := reconcileNode(node.Name)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodeEvents("Normal DrainRequested Requested ")).To(HaveLen(1))
			version := getPod("request-once-db").ResourceVersion
			_, err = reconcileNode(node.Name)
			Expect(err).NotTo(HaveOccurred())
			Expect(getPod("request-once-db").ResourceVersion).To(Equal(version))
			Expect(nodeEvents("Normal DrainRequested Requested ")).To(BeEmpty())
			Expect(counterValue(drainPodActionsTotal, node.Name, "CustomCheck", actionRequestDrain, requestOutcomeRequested)).To(Equal(1.0))
		})

		It("does not patch a pod that already has the annotation key with another value", func() {
			configure(requestConfig("{}", ""), "")
			node := newNode("request-other-value", badCondition("CustomCheck"))
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "request-other-db", Namespace: "default", Labels: map[string]string{"role": "db"}, Annotations: map[string]string{requestKey: "set-by-another-controller"}}, Spec: corev1.PodSpec{NodeName: node.Name, Containers: []corev1.Container{{Name: "c", Image: "busybox"}}}}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod) })
			_, err := reconcileNode(node.Name)
			Expect(err).NotTo(HaveOccurred())
			db := getPod("request-other-db")
			Expect(db.Annotations).To(HaveKeyWithValue(requestKey, "set-by-another-controller"))
			Expect(db.DeletionTimestamp).To(BeNil())
			Expect(counterValue(drainPodActionsTotal, node.Name, "CustomCheck", actionRequestDrain, requestOutcomeRequested)).To(Equal(0.0))
		})

		It("counts a pod that is gone before the annotation patch as removed", func() {
			configure(requestConfig("{}", ""), "")
			node := newNode("request-gone", badCondition("CustomCheck"))
			newPod("request-gone-db", node.Name, map[string]string{"role": "db"})
			reconciler.Client = failingPodPatchClient{Client: k8sClient, err: apierrors.NewNotFound(corev1.Resource("pods"), "request-gone-db")}
			_, err := reconcileNode(node.Name)
			Expect(err).NotTo(HaveOccurred())
			Expect(getNode(node.Name).Annotations).To(HaveKey(AnnotationDrainFinishedAt))
			Expect(counterValue(drainPodActionsTotal, node.Name, "CustomCheck", actionRequestDrain, requestOutcomeError)).To(Equal(0.0))
		})

		It("evicts a matching pod after the drain request timeout", func() {
			configure(requestConfig("{drainTimeout: 1h}", ", timeout: 1m"), "")
			node := startedNode("request-window-passed", time.Hour, 10*time.Minute)
			newPod("request-late-db", node.Name, map[string]string{"role": "db"})
			_, err := reconcileNode(node.Name)
			Expect(err).NotTo(HaveOccurred())
			db := getPod("request-late-db")
			Expect(db.DeletionTimestamp).NotTo(BeNil())
			Expect(db.Annotations).NotTo(HaveKey(requestKey))
			Expect(counterValue(drainPodActionsTotal, node.Name, "CustomCheck", actionEvictPod, evictionOutcomeEvicted)).To(Equal(1.0))
			Expect(counterValue(drainPodActionsTotal, node.Name, "CustomCheck", actionRequestDrain, requestOutcomeRequested)).To(Equal(0.0))
		})

		It("deletes a matching pod after the drain timeout", func() {
			configure(requestConfig("{drainTimeout: 1s}", ", timeout: 1h"), "")
			node := startedNode("request-drain-timeout", time.Second, time.Minute)
			newPod("request-timeout-db", node.Name, map[string]string{"role": "db"})
			_, err := reconcileNode(node.Name)
			Expect(err).NotTo(HaveOccurred())
			Expect(getPod("request-timeout-db").DeletionTimestamp).NotTo(BeNil())
			Expect(counterValue(drainPodActionsTotal, node.Name, "CustomCheck", actionDeletePod, deleteOutcomeDeleted)).To(Equal(1.0))
			Expect(counterValue(drainPodActionsTotal, node.Name, "CustomCheck", actionRequestDrain, requestOutcomeRequested)).To(Equal(0.0))
		})

		It("counts a failed annotation patch and retries it on the next pass", func() {
			configure(requestConfig("{}", ""), "")
			node := newNode("request-patch-error", badCondition("CustomCheck"))
			newPod("request-error-db", node.Name, map[string]string{"role": "db"})
			reconciler.Client = failingPodPatchClient{Client: k8sClient, err: apierrors.NewServiceUnavailable("pod patch unavailable")}
			_, err := reconcileNode(node.Name)
			Expect(err).NotTo(HaveOccurred())
			db := getPod("request-error-db")
			Expect(db.Annotations).NotTo(HaveKey(requestKey))
			Expect(db.DeletionTimestamp).To(BeNil())
			Expect(counterValue(drainPodActionsTotal, node.Name, "CustomCheck", actionRequestDrain, requestOutcomeError)).To(Equal(1.0))
			Expect(nodeEvents("Warning DrainRequestFailed ")).To(HaveLen(1))

			reconciler.Client = k8sClient
			_, err = reconcileNode(node.Name)
			Expect(err).NotTo(HaveOccurred())
			Expect(getPod("request-error-db").Annotations).To(HaveKeyWithValue(requestKey, "true"))
			Expect(counterValue(drainPodActionsTotal, node.Name, "CustomCheck", actionRequestDrain, requestOutcomeRequested)).To(Equal(1.0))
		})

		It("applies the first drain request whose selector matches the pod", func() {
			configure("conditions:\n  CustomCheck: {}\n"+
				"drainRequests:\n"+
				"- {podSelector: {matchLabels: {role: db}}, annotationKey: example.com/first}\n"+
				"- {podSelector: {matchExpressions: [{key: role, operator: Exists}]}, annotationKey: example.com/second}\n", "")
			node := newNode("request-first-match", badCondition("CustomCheck"))
			newPod("request-first-db", node.Name, map[string]string{"role": "db"})
			newPod("request-second-cache", node.Name, map[string]string{"role": "cache"})
			_, err := reconcileNode(node.Name)
			Expect(err).NotTo(HaveOccurred())
			db := getPod("request-first-db")
			Expect(db.Annotations).To(HaveKeyWithValue("example.com/first", "true"))
			Expect(db.Annotations).NotTo(HaveKey("example.com/second"))
			Expect(getPod("request-second-cache").Annotations).To(HaveKeyWithValue("example.com/second", "true"))
			Expect(counterValue(drainPodActionsTotal, node.Name, "CustomCheck", actionRequestDrain, requestOutcomeRequested)).To(Equal(2.0))
		})

		It("evicts a labeled pod when drainRequests is not configured", func() {
			configure("conditions:\n  CustomCheck: {}\n", "")
			node := newNode("request-absent", badCondition("CustomCheck"))
			newPod("request-absent-db", node.Name, map[string]string{"role": "db"})
			_, err := reconcileNode(node.Name)
			Expect(err).NotTo(HaveOccurred())
			db := getPod("request-absent-db")
			Expect(db.DeletionTimestamp).NotTo(BeNil())
			Expect(db.Annotations).NotTo(HaveKey(requestKey))
			Expect(counterValue(drainPodActionsTotal, node.Name, "CustomCheck", actionEvictPod, evictionOutcomeEvicted)).To(Equal(1.0))
		})

		// Without the annotated pod, the terminating pod alone makes the drain stall.
		It("does not report a stall while an annotated pod waits to move", func() {
			configure(requestConfig("{}", "")+"stallTimeout: 1s\n", "")
			node := newNode("request-no-stall", badCondition("CustomCheck"))
			newPod("request-wait-db", node.Name, map[string]string{"role": "db"})
			newPod("request-stuck", node.Name, nil)
			reconciler.APIReader = oldTerminatingNamedPodReader{Reader: k8sClient, name: "request-stuck"}
			_, err := reconcileNode(node.Name)
			Expect(err).NotTo(HaveOccurred())
			Expect(getPod("request-wait-db").Annotations).To(HaveKeyWithValue(requestKey, "true"))
			Expect(testutil.ToFloat64(drainStalledInfo.WithLabelValues(node.Name, "CustomCheck"))).To(Equal(0.0))
			Expect(getNode(node.Name).Annotations).NotTo(HaveKey(AnnotationDrainFinishedAt))
		})
	})
})

var _ = Describe("joinPodRefs", func() {
	It("returns an empty string for no refs", func() { Expect(joinPodRefs(nil, 1024)).To(Equal("")) })
})
