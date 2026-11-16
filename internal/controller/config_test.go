package controller

import (
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

var _ = Describe("readConfig", func() {
	baseConfig := "conditions:\n  KernelDeadlock: {}\n"
	baseData := func() map[string]string {
		return map[string]string{EnabledKey: "true", ConfigKey: baseConfig}
	}
	createCM := func(data map[string]string) {
		_ = k8sClient.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ControllerConfigMap, Namespace: "default"}})
		if data != nil {
			Expect(k8sClient.Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: ControllerConfigMap, Namespace: "default"},
				Data:       data,
			})).To(Succeed())
		}
	}
	readFatalConfig := func(data map[string]string) {
		createCM(data)
		var exitCode int
		osExit = func(code int) { exitCode = code; panic("config rejected") }
		defer func() {
			osExit = os.Exit
			Expect(exitCode).To(Equal(1))
		}()
		Expect(func() { readConfig(ctx, k8sClient, "default") }).To(PanicWith("config rejected"))
	}
	jobs := `diagnostics:
  template:
    spec:
      template:
        spec:
          containers: [{name: run, image: example.test/run}]
mitigate:
  template:
    spec:
      template:
        spec:
          containers: [{name: run, image: example.test/run}]
`
	AfterEach(func() { createCM(baseData()) })

	It("reads omitted taint and drain as enabled", func() {
		createCM(baseData())
		cfg := readConfig(ctx, k8sClient, "default")
		cc := cfg.Conditions["KernelDeadlock"]
		Expect(cc.taints()).To(BeTrue())
		Expect(cc.drains()).To(BeTrue())
	})

	DescribeTable("validates drain settings", func(config string, wantTimeout time.Duration) {
		data := baseData()
		data[ConfigKey] = config
		createCM(data)
		cfg := readConfig(ctx, k8sClient, "default")
		p := planFor(cfg.Conditions["KernelDeadlock"])
		Expect(p.drainTimeout).To(Equal(wantTimeout))
	},
		Entry("negative one means never", "conditions:\n  KernelDeadlock: {drainTimeout: '-1'}\n", drainNever),
		Entry("zero gives up immediately", "conditions:\n  KernelDeadlock: {drainTimeout: '0'}\n", time.Duration(0)),
		Entry("omitted means never", "conditions:\n  KernelDeadlock: {}\n", drainNever),

		Entry("four days", "conditions:\n  KernelDeadlock: {drainTimeout: 4d}\n", 4*24*time.Hour),
		Entry("one week", "conditions:\n  KernelDeadlock: {drainTimeout: 1w}\n", 7*24*time.Hour),
	)

	It("accepts skipPodDeletion with a finite timeout and mitigation Job", func() {
		data := baseData()
		data[ConfigKey] = "conditions:\n  KernelDeadlock: {drainTimeout: 1m, skipPodDeletion: true, mitigationJob: {name: mitigate}}\n"
		data[JobsKey] = jobs
		createCM(data)
		cfg := readConfig(ctx, k8sClient, "default")
		Expect(cfg.Conditions["KernelDeadlock"].SkipPodDeletion).To(BeTrue())
	})

	DescribeTable("rejects invalid condition configuration", func(config, jobsConfig string) {
		data := baseData()
		data[ConfigKey] = config
		if jobsConfig != "" {
			data[JobsKey] = jobsConfig
		}
		readFatalConfig(data)
	},
		Entry("drain without taint", "conditions:\n  KernelDeadlock: {taint: false, drain: true}\n", ""),
		Entry("mitigation Job without taint", "conditions:\n  KernelDeadlock: {taint: false, mitigationJob: {name: mitigate}}\n", jobs),
		Entry("blocking pre-mitigation Job without taint", "conditions:\n  KernelDeadlock: {taint: false, preMitigationJob: {name: diagnostics, failurePolicy: Block}}\n", jobs),
		Entry("skip pod deletion without timeout", "conditions:\n  KernelDeadlock: {skipPodDeletion: true, mitigationJob: {name: mitigate}}\n", jobs),
		Entry("skip pod deletion without mitigation Job", "conditions:\n  KernelDeadlock: {drainTimeout: 1m, skipPodDeletion: true}\n", ""),
		Entry("mitigation Job failure policy", "conditions:\n  KernelDeadlock: {mitigationJob: {name: mitigate, failurePolicy: Block}}\n", jobs),
		Entry("unknown config field", "conditions:\n  KernelDeadlock: {taimt: false}\n", ""),
		Entry("invalid drain timeout below negative one", "conditions:\n  KernelDeadlock: {drainTimeout: -2s}\n", ""),
		Entry("missing excluded taint key", "conditions:\n  KernelDeadlock: {}\nexcludeNodeTaints: [{}]\n", ""),
		Entry("invalid environment variable", "conditions:\n  KernelDeadlock: {jobEnv: {'1BAD': value}}\n", ""),

		Entry("reserved environment variable", "conditions:\n  KernelDeadlock: {jobEnv: {NODE_PROBLEM_MITIGATOR_TEST: value}}\n", ""),
		Entry("invalid Job name", "conditions:\n  KernelDeadlock: {mitigationJob: {name: mitigate}}\n", "NOT VALID: {template: {spec: {template: {spec: {containers: [{name: c, image: i}]}}}}}\n"),
		Entry("Job template without containers", "conditions:\n  KernelDeadlock: {mitigationJob: {name: mitigate}}\n", "mitigate: {template: {spec: {template: {spec: {}}}}}\n"),
		Entry("Job template with ttlSecondsAfterFinished", "conditions:\n  KernelDeadlock: {mitigationJob: {name: mitigate}}\n", "mitigate: {template: {spec: {ttlSecondsAfterFinished: 60, template: {spec: {containers: [{name: c, image: i}]}}}}}\n"),
		Entry("condition name that is not a label value", "conditions:\n  'Kernel Deadlock': {}\n", ""),
	)

	It("parses cooldown, stall timeout, exclusions, and job environment", func() {
		data := baseData()
		data[ConfigKey] = `conditions:
  KernelDeadlock:
    jobEnv: {CUSTOM_SETTING: value}
cooldown: 5m
stallTimeout: 4d
excludeNodeTaints:
  - key: example.com/disrupted
    value: "true"
    effect: NoSchedule
`
		createCM(data)
		cfg := readConfig(ctx, k8sClient, "default")
		Expect(cfg.Cooldown).To(Equal("5m"))
		Expect(cfg.StallTimeout).To(Equal("4d"))
		stall, err := parseOptionalDuration(cfg.StallTimeout)
		Expect(err).NotTo(HaveOccurred())
		Expect(stall).To(Equal(4 * 24 * time.Hour))
		Expect(cfg.ExcludeNodeTaints).To(Equal([]ExcludeTaint{{Key: "example.com/disrupted", Value: "true", Effect: "NoSchedule"}}))
		Expect(cfg.Conditions["KernelDeadlock"].JobEnv).To(HaveKeyWithValue("CUSTOM_SETTING", "value"))
	})

	DescribeTable("rejects invalid optional durations", func(config string) {
		data := baseData()
		data[ConfigKey] = config
		readFatalConfig(data)
	},
		Entry("cooldown", "conditions:\n  KernelDeadlock: {}\ncooldown: invalid\n"),
		Entry("stall timeout", "conditions:\n  KernelDeadlock: {}\nstallTimeout: invalid\n"),
		Entry("job retention", "conditions:\n  KernelDeadlock: {}\njobRetention: invalid\n"),
	)
	It("fills drainRequests defaults and parses each selector", func() {
		data := baseData()
		data[ConfigKey] = baseConfig + "drainRequests:\n- {podSelector: {matchExpressions: [{key: role, operator: In, values: [db, keeper]}]}, annotationKey: ' example.com/drain-requested '}\n- {podSelector: {matchLabels: {role: cache}}, annotationKey: example.com/move, timeout: 5m}\n"
		createCM(data)
		reqs := readConfig(ctx, k8sClient, "default").DrainRequests
		Expect(reqs).To(HaveLen(2))
		req := reqs[0]
		Expect(req.AnnotationKey).To(Equal("example.com/drain-requested"))
		Expect(req.AnnotationValue).To(Equal("true"))
		Expect(req.Timeout).To(Equal("30m"))
		Expect(req.TimeoutDuration).To(Equal(30 * time.Minute))
		Expect(req.Selector.Matches(labels.Set{"role": "keeper"})).To(BeTrue())
		Expect(req.Selector.Matches(labels.Set{"role": "web"})).To(BeFalse())
		Expect(reqs[1].AnnotationKey).To(Equal("example.com/move"))
		Expect(reqs[1].TimeoutDuration).To(Equal(5 * time.Minute))
		Expect(reqs[1].Selector.Matches(labels.Set{"role": "cache"})).To(BeTrue())
	})

	It("leaves drainRequests empty when omitted", func() {
		createCM(baseData())
		Expect(readConfig(ctx, k8sClient, "default").DrainRequests).To(BeEmpty())
	})

	It("accepts a zero drainRequests timeout and a custom value", func() {
		data := baseData()
		data[ConfigKey] = baseConfig + "drainRequests: [{podSelector: {matchLabels: {role: db}}, annotationKey: example.com/move, annotationValue: now, timeout: '0'}]\n"
		createCM(data)
		req := readConfig(ctx, k8sClient, "default").DrainRequests[0]
		Expect(req.AnnotationValue).To(Equal("now"))
		Expect(req.TimeoutDuration).To(Equal(time.Duration(0)))
	})

	DescribeTable("rejects invalid drainRequests", func(block string) {
		data := baseData()
		data[ConfigKey] = baseConfig + "drainRequests: [" + block + "]\n"
		readFatalConfig(data)
	},
		Entry("missing podSelector", "{annotationKey: example.com/move}"),
		Entry("empty podSelector", "{podSelector: {}, annotationKey: example.com/move}"),
		Entry("invalid selector operator", "{podSelector: {matchExpressions: [{key: role, operator: Near, values: [db]}]}, annotationKey: example.com/move}"),
		Entry("missing annotationKey", "{podSelector: {matchLabels: {role: db}}}"),
		Entry("invalid annotationKey", "{podSelector: {matchLabels: {role: db}}, annotationKey: 'not a key!'}"),
		Entry("unparsable timeout", "{podSelector: {matchLabels: {role: db}}, annotationKey: example.com/move, timeout: soon}"),
		Entry("negative timeout", "{podSelector: {matchLabels: {role: db}}, annotationKey: example.com/move, timeout: -1m}"),
		Entry("invalid second entry", "{podSelector: {matchLabels: {role: db}}, annotationKey: example.com/move}, {podSelector: {}, annotationKey: example.com/move}"),
	)

	It("validates pre-mitigation Jobs and defaults failure policy to Continue", func() {
		data := baseData()
		data[ConfigKey] = "conditions:\n  KernelDeadlock: {preMitigationJob: {name: diagnostics}}\n"
		data[JobsKey] = jobs
		createCM(data)
		cfg := readConfig(ctx, k8sClient, "default")
		Expect(cfg.Conditions["KernelDeadlock"].PreMitigationJob.FailurePolicy).To(Equal(PreMitigationJobFailureContinue))
		Expect(cfg.Jobs["diagnostics"].Template.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
	})

	It("validates mitigation Job references", func() {
		data := baseData()
		data[ConfigKey] = "conditions:\n  KernelDeadlock: {mitigationJob: {name: mitigate}}\n"
		data[JobsKey] = jobs
		createCM(data)
		cfg := readConfig(ctx, k8sClient, "default")
		Expect(cfg.Conditions["KernelDeadlock"].MitigationJob.Name).To(Equal("mitigate"))
	})
})
