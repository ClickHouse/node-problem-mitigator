package controller

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"
)

const (
	// PreMitigationJobFailureContinue lets mitigation continue after a pre-mitigation Job fails.
	PreMitigationJobFailureContinue = "Continue"
	// PreMitigationJobFailureBlock stops the mitigation after a pre-mitigation Job fails.
	PreMitigationJobFailureBlock = "Block"

	// defaultPreMitigationJobDeadlineSeconds bounds a Job without an active deadline.
	defaultPreMitigationJobDeadlineSeconds = int64(15 * 60)
	// maxPreMitigationJobDeadlineSeconds limits how long a pre-mitigation Job can run.
	maxPreMitigationJobDeadlineSeconds = int64(60 * 60)

	// jobEnvReservedPrefix starts the names of the variables the controller injects into Jobs.
	jobEnvReservedPrefix = "NODE_PROBLEM_MITIGATOR_"
)

const (
	// ControllerConfigMap is the name of the ConfigMap holding controller configuration.
	ControllerConfigMap = "node-problem-mitigator-config"
	// EnabledKey is the key in the config ConfigMap that controls the kill switch.
	EnabledKey = "enabled"
	// ConfigKey is the key in the ConfigMap holding the structured YAML configuration.
	ConfigKey = "config"
	// JobsKey is the optional ConfigMap key holding named Job templates.
	// Conditions reference them by name.
	JobsKey = "jobs"

	// controllerName is the name of this controller, used in labels and ConfigMap metadata.
	controllerName   = "node-problem-mitigator"
	podNodeNameField = "spec.nodeName"

	// defaultCooldown is used when the config omits a cooldown value.
	defaultCooldown = "15m"

	// defaultStallTimeout is used when the config omits stallTimeout.
	defaultStallTimeout = "5m"

	// defaultJobRetention is used when the config omits jobRetention.
	defaultJobRetention = "1h"

	// defaultDrainRequestTimeout is used when drainRequest omits timeout.
	defaultDrainRequestTimeout = "30m"
	// defaultDrainRequestAnnotationValue is used when drainRequest omits annotationValue.
	defaultDrainRequestAnnotationValue = "true"

	// drainTimeoutNever is the drainTimeout value, and the parsed duration,
	// of a drain that never times out.
	drainTimeoutNever = "-1"
	drainNever        = time.Duration(-1)
)

// osExit is the function called on fatal config errors. Overridden in tests.
var osExit = os.Exit

// ConditionConfig is the per-condition configuration: which mitigation steps
// run and how. Every step is on unless switched off.
type ConditionConfig struct {
	// Taint applies the NoSchedule taint. Defaults to true. False also turns
	// the drain off, leaving only the Jobs and metrics.
	Taint *bool `json:"taint,omitempty"`
	// Drain evicts the node's pods through the Eviction API. Defaults to true.
	Drain *bool `json:"drain,omitempty"`
	// DrainTimeout caps how long the drain evicts pods. After it elapses, the
	// remaining pods are deleted gracefully, bypassing PDBs, unless
	// SkipPodDeletion is set. Accepts Go durations plus "d"/"w" units; empty
	// or "-1" never times out and "0" gives up at once.
	DrainTimeout string `json:"drainTimeout,omitempty"`
	// SkipPodDeletion leaves the remaining pods in place at the drain timeout
	// and launches the mitigation Job. Requires DrainTimeout and MitigationJob.
	SkipPodDeletion bool `json:"skipPodDeletion,omitempty"`
	// PreMitigationJob selects the Job to run before the drain.
	PreMitigationJob JobRef `json:"preMitigationJob,omitempty"`
	// MitigationJob selects the Job to run as the last step.
	MitigationJob JobRef `json:"mitigationJob,omitempty"`
	// For is how long the condition must stay continuously True before its
	// mitigation runs. Accepts Go durations plus "d"/"w" units (e.g. "5m",
	// "4d"); empty or "0" mitigates immediately.
	For string `json:"for,omitempty"`
	// JobEnv adds environment variables to every container of the Jobs this
	// condition launches. Names starting with jobEnvReservedPrefix are rejected.
	JobEnv map[string]string `json:"jobEnv,omitempty"`
}

// taints reports whether the condition taints the node.
func (cc ConditionConfig) taints() bool {
	return cc.Taint == nil || *cc.Taint
}

// drains reports whether the condition drains the node. A condition that does
// not taint never drains.
func (cc ConditionConfig) drains() bool {
	return cc.taints() && (cc.Drain == nil || *cc.Drain)
}

// JobRef selects a Job from the jobs map. FailurePolicy applies to
// pre-mitigation Jobs only.
type JobRef struct {
	// Name is a key of the jobs map.
	Name string `json:"name"`
	// FailurePolicy is Continue (default) or Block.
	FailurePolicy string `json:"failurePolicy,omitempty"`
}

// JobConfig defines a named Job template.
type JobConfig struct {
	Template batchv1.JobTemplateSpec `json:"template"`
}

// ExcludeTaint matches a node taint that excludes the node from mitigation.
// Key must match exactly; Value and Effect, when set, must match too.
type ExcludeTaint struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Effect string `json:"effect,omitempty"`
}

// Config holds the runtime configuration read from the controller ConfigMap.
type Config struct {
	Enabled bool `json:"-"`
	// Conditions maps each NPD condition type to its per-condition
	// configuration. It is both the watch list and the per-condition action; a
	// condition absent from the map is not watched. An operator can start any
	// entry by name, including one no node reports.
	Conditions map[string]ConditionConfig `json:"conditions"`
	// Jobs is read from JobsKey, not from the config key.
	Jobs              map[string]*JobConfig `json:"-"`
	Cooldown          string                `json:"cooldown,omitempty"`
	ExcludeNamespaces []string              `json:"excludeNamespaces,omitempty"`
	ExcludeNodeLabels map[string]string     `json:"excludeNodeLabels,omitempty"`
	ExcludeNodeTaints []ExcludeTaint        `json:"excludeNodeTaints,omitempty"`
	// ExcludeNodeAnnotations skips nodes carrying any of the listed key: value
	// annotations (e.g. karpenter.sh/do-not-disrupt: "true").
	ExcludeNodeAnnotations map[string]string `json:"excludeNodeAnnotations,omitempty"`
	// StallTimeout declares a drain stalled once every evictable pod left on the
	// node has been terminating for this long past its deletionTimestamp.
	// Accepts Go durations plus "d"/"w" units; empty means defaultStallTimeout,
	// "0" never stalls.
	StallTimeout string `json:"stallTimeout,omitempty"`
	// JobRetention is how long a finished Job is kept before the controller
	// deletes it. A failed Job of a node's current mitigation is kept while the
	// node exists. Accepts Go durations plus "d"/"w" units; empty means
	// defaultJobRetention, "0" deletes at once.
	JobRetention string `json:"jobRetention,omitempty"`
	// DrainRequests are tried in order. The first entry whose podSelector
	// matches a pod applies to that pod.
	DrainRequests []DrainRequestConfig `json:"drainRequests,omitempty"`
}

// DrainRequestConfig makes a drain annotate matching pods, so another
// controller moves them, and evict them only after Timeout from the drain start.
type DrainRequestConfig struct {
	PodSelector     *metav1.LabelSelector `json:"podSelector"`
	AnnotationKey   string                `json:"annotationKey"`
	AnnotationValue string                `json:"annotationValue,omitempty"`
	Timeout         string                `json:"timeout,omitempty"`

	// Selector and TimeoutDuration are parsed once by readConfig.
	Selector        labels.Selector `json:"-"`
	TimeoutDuration time.Duration   `json:"-"`
}

// readConfig reads the ConfigMap and returns a Config.
//
// If the ConfigMap is not found (e.g. deleted at runtime), returns Config{Enabled: false}
// so the controller is effectively disabled (fail-open).
// Crashes (os.Exit) if the ConfigMap exists but is malformed.
func readConfig(ctx context.Context, c client.Reader, namespace string) Config {
	log := logf.FromContext(ctx)
	var cm corev1.ConfigMap
	err := c.Get(ctx, types.NamespacedName{
		Name:      ControllerConfigMap,
		Namespace: namespace,
	}, &cm)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Config ConfigMap not found, treating as disabled")
			return Config{}
		}
		log.Error(err, "FATAL: cannot read config ConfigMap, crashing")
		osExit(1)
		return Config{}
	}
	// enabled (required, separate key for ArgoCD ignoreDifferences)
	enabledVal, ok := cm.Data[EnabledKey]
	if !ok {
		log.Error(fmt.Errorf("missing key %q in ConfigMap %s", EnabledKey, ControllerConfigMap),
			"FATAL: invalid config ConfigMap, crashing")
		osExit(1)
	}

	// config (required, structured YAML)
	configRaw, ok := cm.Data[ConfigKey]
	if !ok || strings.TrimSpace(configRaw) == "" {
		log.Error(fmt.Errorf("missing or empty key %q in ConfigMap %s", ConfigKey, ControllerConfigMap),
			"FATAL: invalid config ConfigMap, crashing")
		osExit(1)
	}

	var cfg Config
	// Strict: a misspelled step key would otherwise fall back to the default,
	// which taints and drains.
	if err := yaml.UnmarshalStrict([]byte(configRaw), &cfg); err != nil {
		log.Error(fmt.Errorf("cannot parse %q in ConfigMap %s: %w", ConfigKey, ControllerConfigMap, err),
			"FATAL: invalid config ConfigMap, crashing")
		osExit(1)
	}

	// jobs (optional, structured YAML)
	if jobsRaw := cm.Data[JobsKey]; strings.TrimSpace(jobsRaw) != "" {
		cfg.readJobs(ctx, jobsRaw)
	}

	cfg.Enabled = strings.EqualFold(enabledVal, "true")

	cfg.readConditions(ctx)

	// Exclude taints: key is required.
	for i, et := range cfg.ExcludeNodeTaints {
		if strings.TrimSpace(et.Key) == "" {
			log.Error(fmt.Errorf("excludeNodeTaints[%d] is missing key in ConfigMap %s", i, ControllerConfigMap),
				"FATAL: invalid config ConfigMap, crashing")
			osExit(1)
		}
	}

	// Cooldown: validate and default.
	if cd := strings.TrimSpace(cfg.Cooldown); cd == "" {
		cfg.Cooldown = defaultCooldown
	} else {
		if _, err := time.ParseDuration(cd); err != nil {
			log.Error(fmt.Errorf("cannot parse cooldown %q in ConfigMap %s: %w", cd, ControllerConfigMap, err),
				"FATAL: invalid config ConfigMap, crashing")
			osExit(1)
		}
		cfg.Cooldown = cd
	}

	// Stall timeout: validate and default. An explicit "0" is kept and disables
	// the stall check.
	if st := strings.TrimSpace(cfg.StallTimeout); st == "" {
		cfg.StallTimeout = defaultStallTimeout
	} else {
		if _, err := parseOptionalDuration(st); err != nil {
			log.Error(fmt.Errorf("cannot parse stallTimeout %q in ConfigMap %s: %w", st, ControllerConfigMap, err),
				"FATAL: invalid config ConfigMap, crashing")
			osExit(1)
		}
		cfg.StallTimeout = st
	}

	if jr := strings.TrimSpace(cfg.JobRetention); jr == "" {
		cfg.JobRetention = defaultJobRetention
	} else {
		if _, err := parseOptionalDuration(jr); err != nil {
			log.Error(fmt.Errorf("cannot parse jobRetention %q in ConfigMap %s: %w", jr, ControllerConfigMap, err),
				"FATAL: invalid config ConfigMap, crashing")
			osExit(1)
		}
		cfg.JobRetention = jr
	}

	cfg.readDrainRequests(ctx)

	return cfg
}

// jobRetention returns how long finished Jobs are kept. A Config read without
// a ConfigMap keeps the default.
func (cfg Config) jobRetention() time.Duration {
	value := cfg.JobRetention
	if strings.TrimSpace(value) == "" {
		value = defaultJobRetention
	}
	d, _ := parseOptionalDuration(value)
	return d
}

// readConditions validates the conditions. Steps must be consistent and every
// Job reference must resolve. Any violation is fatal.
func (cfg *Config) readConditions(ctx context.Context) {
	log := logf.FromContext(ctx)
	fatal := func(format string, args ...any) {
		log.Error(fmt.Errorf(format+" in ConfigMap %s", append(args, ControllerConfigMap)...),
			"FATAL: invalid config ConfigMap, crashing")
		osExit(1)
	}
	for cond, cc := range cfg.Conditions {
		// The name labels the condition's Jobs.
		if errs := validation.IsValidLabelValue(cond); cond == "" || len(errs) > 0 {
			fatal("condition %q is not a valid label value: %s", cond, strings.Join(errs, "; "))
		}
		if !cc.taints() && cc.Drain != nil && *cc.Drain {
			fatal("condition %q sets drain without taint", cond)
		}
		if !cc.taints() && cc.MitigationJob != (JobRef{}) {
			fatal("condition %q sets mitigationJob without taint", cond)
		}
		if !cc.taints() && cc.PreMitigationJob.FailurePolicy == PreMitigationJobFailureBlock {
			fatal("condition %q sets preMitigationJob failurePolicy Block without taint", cond)
		}
		drainTimeout, err := parseDrainTimeout(cc.DrainTimeout)
		if err != nil {
			fatal("cannot parse drainTimeout %q for condition %q: %v", cc.DrainTimeout, cond, err)
		}
		if !cc.drains() && (cc.DrainTimeout != "" || cc.SkipPodDeletion) {
			fatal("condition %q sets drainTimeout or skipPodDeletion without a drain", cond)
		}
		if cc.SkipPodDeletion && (drainTimeout < 0 || cc.MitigationJob.Name == "") {
			fatal("skipPodDeletion requires a drainTimeout and a mitigationJob for condition %q", cond)
		}
		if _, err := parseOptionalDuration(cc.For); err != nil {
			fatal("cannot parse for %q for condition %q: %v", cc.For, cond, err)
		}
		cc.PreMitigationJob = cfg.validatePreMitigationJobRef(ctx, cond, cc.PreMitigationJob)
		if ref := cc.MitigationJob; ref != (JobRef{}) {
			if ref.FailurePolicy != "" {
				fatal("mitigationJob for condition %q does not accept failurePolicy", cond)
			}
			if cfg.Jobs[ref.Name] == nil {
				fatal("mitigationJob %q for condition %q is not defined in %q key", ref.Name, cond, JobsKey)
			}
		}
		for name := range cc.JobEnv {
			if errs := validation.IsEnvVarName(name); len(errs) > 0 || strings.HasPrefix(name, jobEnvReservedPrefix) {
				fatal("invalid jobEnv name %q for condition %q: must be a valid environment variable name without the %s prefix", name, cond, jobEnvReservedPrefix)
			}
		}
		cfg.Conditions[cond] = cc
	}

	if cfg.Enabled && len(cfg.Conditions) == 0 {
		fatal("no conditions configured while controller is enabled in %q key", ConfigKey)
	}
}

// readJobs parses the optional jobs key. An empty or overlong name, a template
// without containers, or one that sets ttlSecondsAfterFinished is fatal.
func (cfg *Config) readJobs(ctx context.Context, raw string) {
	log := logf.FromContext(ctx)
	if err := yaml.Unmarshal([]byte(raw), &cfg.Jobs); err != nil {
		log.Error(fmt.Errorf("cannot parse %q in ConfigMap %s: %w", JobsKey, ControllerConfigMap, err),
			"FATAL: invalid config ConfigMap, crashing")
		osExit(1)
	}
	for _, name := range slices.Sorted(maps.Keys(cfg.Jobs)) {
		// The name becomes part of the node's Job annotation keys and a Job label.
		errs := validation.IsQualifiedName(mitigationJobLaunchedAnnotation(name))
		errs = append(errs, validation.IsValidLabelValue(name)...)
		if strings.TrimSpace(name) == "" || len(errs) > 0 {
			log.Error(fmt.Errorf("%q entry %q is not a valid Job name in ConfigMap %s: %s", JobsKey, name, ControllerConfigMap, strings.Join(errs, "; ")),
				"FATAL: invalid config ConfigMap, crashing")
			osExit(1)
		}
		job := cfg.Jobs[name]
		if job == nil || len(job.Template.Spec.Template.Spec.Containers) == 0 {
			log.Error(fmt.Errorf("%q entry %q template must define at least one container in ConfigMap %s", JobsKey, name, ControllerConfigMap),
				"FATAL: invalid config ConfigMap, crashing")
			osExit(1)
			continue
		}
		if job.Template.Spec.TTLSecondsAfterFinished != nil {
			log.Error(fmt.Errorf("%q entry %q sets ttlSecondsAfterFinished in ConfigMap %s; the controller deletes finished Jobs after jobRetention", JobsKey, name, ControllerConfigMap),
				"FATAL: invalid config ConfigMap, crashing")
			osExit(1)
		}
		if job.Template.Spec.Template.Spec.RestartPolicy == "" {
			job.Template.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
		}
	}
}

// validatePreMitigationJobRef checks a condition's pre-mitigation Job reference
// and defaults its failure policy to Continue. An empty reference selects no
// Job. The Job's deadline must be 1s..1h.
func (cfg Config) validatePreMitigationJobRef(ctx context.Context, condition string, ref JobRef) JobRef {
	if ref == (JobRef{}) {
		return ref
	}
	log := logf.FromContext(ctx)
	job := cfg.Jobs[ref.Name]
	if job == nil {
		log.Error(fmt.Errorf("preMitigationJob %q for condition %q is not defined in %q key of ConfigMap %s", ref.Name, condition, JobsKey, ControllerConfigMap),
			"FATAL: invalid config ConfigMap, crashing")
		osExit(1)
		return ref
	}
	if deadline := job.Template.Spec.ActiveDeadlineSeconds; deadline != nil && (*deadline <= 0 || *deadline > maxPreMitigationJobDeadlineSeconds) {
		log.Error(fmt.Errorf("pre-mitigation Job %q activeDeadlineSeconds must be between 1 and %d in ConfigMap %s", ref.Name, maxPreMitigationJobDeadlineSeconds, ControllerConfigMap),
			"FATAL: invalid config ConfigMap, crashing")
		osExit(1)
	}
	switch ref.FailurePolicy {
	case "":
		ref.FailurePolicy = PreMitigationJobFailureContinue
	case PreMitigationJobFailureContinue, PreMitigationJobFailureBlock:
	default:
		log.Error(fmt.Errorf("unsupported failurePolicy %q in preMitigationJob for condition %q in ConfigMap %s", ref.FailurePolicy, condition, ControllerConfigMap),
			"FATAL: invalid config ConfigMap, crashing")
		osExit(1)
	}
	return ref
}

// readDrainRequests validates each drainRequests entry and fills its defaults and parsed fields.
func (cfg *Config) readDrainRequests(ctx context.Context) {
	for i := range cfg.DrainRequests {
		if !readDrainRequest(ctx, i, &cfg.DrainRequests[i]) {
			return
		}
	}
}

// readDrainRequest validates one drainRequests entry. It returns false after a fatal error.
func readDrainRequest(ctx context.Context, i int, req *DrainRequestConfig) bool {
	fatal := func(err error) bool {
		logf.FromContext(ctx).Error(fmt.Errorf("drainRequests[%d] in ConfigMap %s: %w", i, ControllerConfigMap, err),
			"FATAL: invalid config ConfigMap, crashing")
		osExit(1)
		return false
	}
	// An empty selector matches every pod.
	if req.PodSelector == nil || len(req.PodSelector.MatchLabels)+len(req.PodSelector.MatchExpressions) == 0 {
		return fatal(fmt.Errorf("podSelector is required and must not be empty"))
	}
	sel, err := metav1.LabelSelectorAsSelector(req.PodSelector)
	if err != nil {
		return fatal(fmt.Errorf("invalid podSelector: %w", err))
	}
	req.Selector = sel
	req.AnnotationKey = strings.TrimSpace(req.AnnotationKey)
	if errs := validation.IsQualifiedName(req.AnnotationKey); len(errs) > 0 {
		return fatal(fmt.Errorf("invalid annotationKey %q: %s", req.AnnotationKey, strings.Join(errs, "; ")))
	}
	if req.AnnotationValue == "" {
		req.AnnotationValue = defaultDrainRequestAnnotationValue
	}
	if t := strings.TrimSpace(req.Timeout); t == "" {
		req.Timeout = defaultDrainRequestTimeout
	} else {
		req.Timeout = t
	}
	d, err := parseOptionalDuration(req.Timeout)
	if err != nil {
		return fatal(fmt.Errorf("cannot parse timeout %q: %w", req.Timeout, err))
	}
	req.TimeoutDuration = d
	return true
}

// parseOptionalDuration parses a duration string; an empty value or "0" yields
// the zero duration. Unlike time.ParseDuration it also accepts day ("d") and
// week ("w") units via prometheus/common model durations, e.g. "30m", "4d".
func parseOptionalDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil
	}
	d, err := model.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	return time.Duration(d), nil
}

// parseDrainTimeout parses a drain timeout: empty or "-1" yields drainNever,
// "0" gives up at once, and other values are durations as in
// parseOptionalDuration.
func parseDrainTimeout(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == drainTimeoutNever {
		return drainNever, nil
	}
	return parseOptionalDuration(s)
}
