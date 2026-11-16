package controller

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"
)

// plan is the set of mitigation steps a node runs. It merges every condition
// that has owned the node, and each knob keeps its most aggressive value, so a
// mitigation never steps back.
type plan struct {
	taint bool
	drain bool
	// drainTimeout is drainNever when the drain never times out.
	drainTimeout    time.Duration
	skipPodDeletion bool
	// failurePolicy applies to a failed pre-mitigation Job: Continue, Block,
	// or empty when no condition in the plan has a pre-mitigation Job.
	failurePolicy string
	// job is the mitigation Job, a key of the jobs map.
	job string
}

// Plan keys in the AnnotationMitigationPlan value. They match the condition
// configuration fields.
const (
	planTaint           = "taint"
	planDrain           = "drain"
	planDrainTimeout    = "drainTimeout"
	planSkipPodDeletion = "skipPodDeletion"
	planFailurePolicy   = "failurePolicy"
	planJob             = "job"
)

// planFor returns the steps a condition's configuration selects. readConfig
// has validated the configuration.
func planFor(cc ConditionConfig) plan {
	p := plan{
		taint:           cc.taints(),
		drain:           cc.drains(),
		drainTimeout:    drainNever,
		skipPodDeletion: cc.SkipPodDeletion,
		failurePolicy:   cc.PreMitigationJob.FailurePolicy,
		job:             cc.MitigationJob.Name,
	}
	if p.drain {
		p.drainTimeout, _ = parseDrainTimeout(cc.DrainTimeout)
	}
	return p
}

// merge returns the plan with each knob at its more aggressive value: taint,
// drain and skipPodDeletion are ORed, the shorter drain timeout wins, Continue
// beats Block, and an existing mitigation Job is kept.
func (p plan) merge(q plan) plan {
	out := plan{
		taint:           p.taint || q.taint,
		drain:           p.drain || q.drain,
		drainTimeout:    p.drainTimeout,
		skipPodDeletion: p.skipPodDeletion || q.skipPodDeletion,
		failurePolicy:   p.failurePolicy,
		job:             p.job,
	}
	if q.failurePolicy == PreMitigationJobFailureContinue || out.failurePolicy == "" {
		out.failurePolicy = q.failurePolicy
	}
	if q.drainTimeout >= 0 && (out.drainTimeout < 0 || q.drainTimeout < out.drainTimeout) {
		out.drainTimeout = q.drainTimeout
	}
	if out.job == "" {
		out.job = q.job
	}
	return out
}

// String encodes the plan for AnnotationMitigationPlan in short form, for
// example "taint,drain,drainTimeout=30m,failurePolicy=Continue,job=terminate".
// taint and drain are always written; other steps only when set.
func (p plan) String() string {
	flag := func(key string, on bool) string {
		if on {
			return key
		}
		return key + "=false"
	}
	tokens := []string{flag(planTaint, p.taint), flag(planDrain, p.drain)}
	if p.drainTimeout >= 0 {
		tokens = append(tokens, planDrainTimeout+"="+model.Duration(p.drainTimeout).String())
	}
	if p.skipPodDeletion {
		tokens = append(tokens, planSkipPodDeletion)
	}
	if p.failurePolicy != "" {
		tokens = append(tokens, planFailurePolicy+"="+p.failurePolicy)
	}
	if p.job != "" {
		tokens = append(tokens, planJob+"="+p.job)
	}
	return strings.Join(tokens, ",")
}

// parsePlan decodes an AnnotationMitigationPlan value. A bare taint, drain or
// skipPodDeletion means true. An absent key takes the condition configuration
// default: taint, drain when tainting, no timeout.
func parsePlan(s string) (plan, error) {
	p := plan{taint: true, drainTimeout: drainNever}
	var drain *bool
	for token := range strings.SplitSeq(s, ",") {
		if token = strings.TrimSpace(token); token == "" {
			continue
		}
		key, value, ok := strings.Cut(token, "=")
		if !ok {
			switch key {
			case planTaint, planDrain, planSkipPodDeletion:
				value = "true"
			default:
				return plan{}, fmt.Errorf("plan %q: step %q has no value", s, token)
			}
		}
		var err error
		switch key {
		case planTaint:
			p.taint, err = strconv.ParseBool(value)
		case planDrain:
			var d bool
			d, err = strconv.ParseBool(value)
			drain = &d
		case planDrainTimeout:
			p.drainTimeout, err = parseDrainTimeout(value)
		case planSkipPodDeletion:
			p.skipPodDeletion, err = strconv.ParseBool(value)
		case planFailurePolicy:
			if value != PreMitigationJobFailureContinue && value != PreMitigationJobFailureBlock {
				err = fmt.Errorf("unknown failure policy %q", value)
			}
			p.failurePolicy = value
		case planJob:
			if value == "" {
				err = errors.New("empty job")
			}
			p.job = value
		default:
			err = fmt.Errorf("unknown step %q", key)
		}
		if err != nil {
			return plan{}, fmt.Errorf("plan %q: %w", s, err)
		}
	}
	p.drain = p.taint
	if drain != nil {
		p.drain = *drain
	}
	return p, nil
}

// takeover merges the owner's current configuration and then each eligible
// condition, in name order, into the plan. A condition becomes the owner
// when it makes the plan more aggressive. An owner missing from the
// configuration keeps its stored plan.
func takeover(owner string, p plan, candidates []string, cfg Config) (string, plan) {
	if cc, ok := cfg.Conditions[owner]; ok {
		p = p.merge(planFor(cc))
	}
	for _, condition := range candidates {
		if condition == owner {
			continue
		}
		if merged := p.merge(planFor(cfg.Conditions[condition])); merged != p {
			owner, p = condition, merged
		}
	}
	return owner, p
}
