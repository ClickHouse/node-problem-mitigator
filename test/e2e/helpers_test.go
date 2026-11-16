//go:build e2e

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/clickhouse/node-problem-mitigator/internal/controller"
)

// fakeNodeLabel limits cleanup to nodes created by this suite.
const fakeNodeLabel = "node-problem-mitigator-e2e/fake"

// createFakeNode waits for KWOK to make a simulated node Ready.
// Its taint keeps the controller and real Jobs off simulated nodes.
func createFakeNode(name string) types.NamespacedName {
	GinkgoHelper()
	resources := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("4"),
		corev1.ResourceMemory: resource.MustParse("8Gi"),
		corev1.ResourcePods:   resource.MustParse("64"),
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"kubernetes.io/os":       "linux",
				"kubernetes.io/role":     "agent",
				fakeNodeLabel:            "true",
				"kubernetes.io/hostname": name,
			},
			Annotations: map[string]string{"kwok.x-k8s.io/node": "fake"},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{
				{Key: "kwok.x-k8s.io/node", Value: "fake", Effect: corev1.TaintEffectNoSchedule},
			},
		},
		Status: corev1.NodeStatus{Capacity: resources, Allocatable: resources},
	}
	Expect(k8sClient.Create(testCtx, node)).To(Succeed())
	key := types.NamespacedName{Name: name}
	Eventually(func() (bool, error) {
		var ready corev1.Node
		if err := k8sClient.Get(testCtx, key, &ready); err != nil {
			return false, err
		}
		for _, condition := range ready.Status.Conditions {
			if condition.Type == corev1.NodeReady {
				return condition.Status == corev1.ConditionTrue, nil
			}
		}
		return false, nil
	}, time.Minute, 250*time.Millisecond).Should(BeTrue(), "KWOK should make the node Ready")
	return key
}

// setBadCondition adds or updates a condition=True on the node's status subresource.
// Retries on conflict (409) since the controller may be updating the same node concurrently.
func setBadCondition(key types.NamespacedName, condType corev1.NodeConditionType) {
	GinkgoHelper()
	Eventually(func() error {
		var node corev1.Node
		if err := k8sClient.Get(testCtx, key, &node); err != nil {
			return err
		}

		now := metav1.NewTime(time.Now())
		found := false
		for i := range node.Status.Conditions {
			if node.Status.Conditions[i].Type == condType {
				node.Status.Conditions[i].Status = corev1.ConditionTrue
				node.Status.Conditions[i].Reason = string(condType) + "Active"
				node.Status.Conditions[i].LastHeartbeatTime = now
				node.Status.Conditions[i].LastTransitionTime = now
				found = true
				break
			}
		}
		if !found {
			node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{
				Type:               condType,
				Status:             corev1.ConditionTrue,
				Reason:             string(condType) + "Active",
				LastHeartbeatTime:  now,
				LastTransitionTime: now,
			})
		}
		return k8sClient.Status().Update(testCtx, &node)
	}, 5*time.Second, 250*time.Millisecond).Should(Succeed())
}

// hasMitigatorTaint returns true if the node carries our taint.
func hasMitigatorTaint(key types.NamespacedName) bool {
	var node corev1.Node
	if err := k8sClient.Get(testCtx, key, &node); err != nil {
		return false
	}
	for _, t := range node.Spec.Taints {
		if t.Key == controller.TaintKey && t.Effect == controller.TaintEffect {
			return true
		}
	}
	return false
}

// countTaintedNodes returns the number of fake nodes (keys in list) that
// currently carry the mitigator taint.
func countTaintedNodes(keys []types.NamespacedName) int {
	n := 0
	for _, k := range keys {
		if hasMitigatorTaint(k) {
			n++
		}
	}
	return n
}

// cleanupFakeNodes removes only nodes created by this suite.
func cleanupFakeNodes() {
	GinkgoHelper()
	var nodes corev1.NodeList
	Expect(k8sClient.List(testCtx, &nodes, client.MatchingLabels{fakeNodeLabel: "true"})).To(Succeed())
	for i := range nodes.Items {
		Expect(client.IgnoreNotFound(k8sClient.Delete(testCtx, &nodes.Items[i]))).To(Succeed())
	}
}

func waitForCooldown() {
	GinkgoHelper()
	Eventually(func() (bool, error) {
		var state corev1.ConfigMap
		key := types.NamespacedName{Name: controller.StateConfigMap, Namespace: StateNamespace}
		err := k8sClient.Get(testCtx, key, &state)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		last, err := time.Parse(time.RFC3339, state.Data["last-mitigation-time"])
		if err != nil {
			return false, err
		}
		return time.Since(last) > TestCooldown+time.Second, nil
	}, TestCooldown+15*time.Second, 250*time.Millisecond).Should(BeTrue())
}

func nodeAnnotation(key types.NamespacedName, name string) (string, error) {
	var node corev1.Node
	if err := k8sClient.Get(testCtx, key, &node); err != nil {
		return "", err
	}
	return node.Annotations[name], nil
}

// expectRealJobCompleted waits for the node's Job with the given component
// label to succeed.
func expectRealJobCompleted(key types.NamespacedName, component string) {
	GinkgoHelper()
	var job batchv1.Job
	Eventually(func(g Gomega) {
		var jobs batchv1.JobList
		g.Expect(k8sClient.List(testCtx, &jobs, client.InNamespace(StateNamespace),
			client.MatchingLabels{"app.kubernetes.io/component": component})).To(Succeed())
		var found []batchv1.Job
		for _, j := range jobs.Items {
			if j.Annotations["clickhouse.com/npm-target-node"] == key.Name {
				found = append(found, j)
			}
		}
		g.Expect(found).To(HaveLen(1), "the controller should launch one %s Job for the node", component)
		job = found[0]
		g.Expect(job.Status.Succeeded).To(Equal(int32(1)))
	}, 3*time.Minute, time.Second).Should(Succeed())

	var pods corev1.PodList
	Expect(k8sClient.List(testCtx, &pods, client.InNamespace(StateNamespace),
		client.MatchingLabels{batchv1.JobNameLabel: job.Name})).To(Succeed())
	Expect(pods.Items).To(HaveLen(1))
	Expect(pods.Items[0].Status.Phase).To(Equal(corev1.PodSucceeded))
	var worker corev1.Node
	Expect(k8sClient.Get(testCtx, types.NamespacedName{Name: pods.Items[0].Spec.NodeName}, &worker)).To(Succeed())
	Expect(worker.Annotations).NotTo(HaveKeyWithValue("kwok.x-k8s.io/node", "fake"),
		"action Jobs must execute on a real kind node")
}

func fakeNodeName(prefix string, i int) string {
	return fmt.Sprintf("%s-%d", prefix, i)
}
