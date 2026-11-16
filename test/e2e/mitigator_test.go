//go:build e2e

package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Helm-installed node-problem-mitigator", func() {
	BeforeEach(func() {
		cleanupFakeNodes()
		waitForCooldown()
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			cleanupFakeNodes()
		}
	})

	It("taints a KWOK node after a configured condition becomes True", func() {
		key := createFakeNode("single-bad")
		Consistently(func() bool { return hasMitigatorTaint(key) }, 2*time.Second, 250*time.Millisecond).
			Should(BeFalse(), "a healthy node should remain untainted")

		setBadCondition(key, "FrequentKubeletRestart")
		Eventually(func() bool { return hasMitigatorTaint(key) }, 30*time.Second, 250*time.Millisecond).
			Should(BeTrue())
	})

	It("taints at most one KWOK node per cooldown window", func() {
		const n = 5
		keys := make([]types.NamespacedName, 0, n)
		for i := range n {
			keys = append(keys, createFakeNode(fakeNodeName("burst", i)))
		}
		for _, key := range keys {
			setBadCondition(key, "FrequentKubeletRestart")
		}

		Eventually(func() int { return countTaintedNodes(keys) }, 15*time.Second, 250*time.Millisecond).
			Should(Equal(1))
		Consistently(func() int { return countTaintedNodes(keys) }, TestCooldown-5*time.Second, 500*time.Millisecond).
			Should(Equal(1), "the global cooldown should prevent a second taint")
		Eventually(func() int { return countTaintedNodes(keys) }, TestCooldown+15*time.Second, 500*time.Millisecond).
			Should(BeNumerically(">=", 2))
	})

	It("runs real pre-mitigation and mitigation Jobs around a PDB-protected KWOK drain", func() {
		key := createFakeNode("drain-with-jobs")
		var node corev1.Node
		Expect(k8sClient.Get(testCtx, key, &node)).To(Succeed())
		labels := map[string]string{"node-problem-mitigator-e2e/workload": key.Name}
		grace := int64(0)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "protected-workload", Namespace: StateNamespace, Labels: labels},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{"kubernetes.io/hostname": key.Name},
				Tolerations: []corev1.Toleration{{
					Key: "kwok.x-k8s.io/node", Operator: corev1.TolerationOpEqual,
					Value: "fake", Effect: corev1.TaintEffectNoSchedule,
				}},
				TerminationGracePeriodSeconds: &grace,
				Containers:                    []corev1.Container{{Name: "workload", Image: "registry.k8s.io/pause:3.10"}},
			},
		}
		Expect(k8sClient.Create(testCtx, pod)).To(Succeed())
		DeferCleanup(func() {
			if !CurrentSpecReport().Failed() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(testCtx, pod))).To(Succeed())
			}
		})
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(testCtx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).To(Equal(key.Name))
			g.Expect(pod.Status.Phase).To(Equal(corev1.PodRunning))
		}, time.Minute, 250*time.Millisecond).Should(Succeed(), "KWOK should run the simulated workload")

		minimum := intstr.FromInt32(1)
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Name: "protected-workload", Namespace: StateNamespace},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: &minimum,
				Selector:     &metav1.LabelSelector{MatchLabels: labels},
			},
		}
		Expect(k8sClient.Create(testCtx, pdb)).To(Succeed())
		DeferCleanup(func() {
			if !CurrentSpecReport().Failed() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(testCtx, pdb))).To(Succeed())
			}
		})
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(testCtx, client.ObjectKeyFromObject(pdb), pdb)).To(Succeed())
			g.Expect(pdb.Status.CurrentHealthy).To(Equal(int32(1)))
			g.Expect(pdb.Status.DisruptionsAllowed).To(BeZero())
		}, time.Minute, 250*time.Millisecond).Should(Succeed())

		By("observing the pre-mitigation Job execute on a real kind node")
		setBadCondition(key, "E2EDrain")
		expectRealJobCompleted(key, "pre-mitigation")
		Eventually(func() (string, error) {
			return nodeAnnotation(key, "clickhouse.com/node-pre-mitigation-job-outcome")
		}, time.Minute, 250*time.Millisecond).Should(Equal("succeeded"))
		Expect(nodeAnnotation(key, "node-problem-mitigator-e2e/pre-mitigation")).To(Equal("complete"))
		Expect(hasMitigatorTaint(key)).To(BeTrue())
		setBadCondition(key, "E2EDrain")

		By("observing a real Eviction API rejection from the PodDisruptionBudget")
		Eventually(func() (bool, error) {
			var events corev1.EventList
			if err := k8sClient.List(testCtx, &events); err != nil {
				return false, err
			}
			for _, event := range events.Items {
				if event.InvolvedObject.UID == node.UID && event.Reason == "EvictionBlocked" {
					return true, nil
				}
			}
			return false, nil
		}, time.Minute, 500*time.Millisecond).Should(BeTrue())
		Consistently(func(g Gomega) {
			g.Expect(k8sClient.Get(testCtx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
			g.Expect(pod.DeletionTimestamp).To(BeNil())
			g.Expect(nodeAnnotation(key, "node-problem-mitigator-e2e/mitigation")).To(BeEmpty())
			g.Expect(nodeAnnotation(key, "clickhouse.com/node-mitigation-job-launched-mark-node")).To(BeEmpty())
		}, 3*time.Second, 250*time.Millisecond).Should(Succeed())

		By("allowing eviction and observing the mitigation Job execute")
		Expect(k8sClient.Delete(testCtx, pdb)).To(Succeed())
		setBadCondition(key, "E2EDrain")
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(testCtx, client.ObjectKeyFromObject(pod), &corev1.Pod{}))
		}, time.Minute, 250*time.Millisecond).Should(BeTrue())
		expectRealJobCompleted(key, "mitigation")
		Eventually(func() (bool, error) {
			var events corev1.EventList
			if err := k8sClient.List(testCtx, &events); err != nil {
				return false, err
			}
			for _, event := range events.Items {
				if event.InvolvedObject.UID == node.UID && event.Reason == "MitigationJobSucceeded" {
					return true, nil
				}
			}
			return false, nil
		}, time.Minute, 250*time.Millisecond).Should(BeTrue())
		Expect(nodeAnnotation(key, "node-problem-mitigator-e2e/mitigation")).To(Equal("complete"))
	})
})
