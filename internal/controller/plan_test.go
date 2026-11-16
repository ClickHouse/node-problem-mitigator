package controller

import (
	"testing"
	"time"
)

func TestPlanMerge(t *testing.T) {
	tests := []struct {
		name  string
		left  plan
		right plan
		want  plan
	}{
		{
			name:  "combine steps and choose the shorter timeout",
			left:  plan{taint: true, drain: true, drainTimeout: 30 * time.Minute, failurePolicy: PreMitigationJobFailureBlock, job: "first"},
			right: plan{drain: true, drainTimeout: 5 * time.Minute, skipPodDeletion: true, failurePolicy: PreMitigationJobFailureContinue, job: "second"},
			want:  plan{taint: true, drain: true, drainTimeout: 5 * time.Minute, skipPodDeletion: true, failurePolicy: PreMitigationJobFailureContinue, job: "first"},
		},
		{
			name:  "keep the finite timeout and first Job",
			left:  plan{drainTimeout: 5 * time.Minute, job: "first"},
			right: plan{drainTimeout: drainNever, job: "second"},
			want:  plan{drainTimeout: 5 * time.Minute, job: "first"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.left.merge(tt.right)
			if got != tt.want {
				t.Fatalf("merge() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestPlanStringRoundTrip(t *testing.T) {
	tests := []plan{
		{drainTimeout: drainNever},
		{taint: true, drain: true, drainTimeout: 30 * time.Minute, skipPodDeletion: true, failurePolicy: PreMitigationJobFailureBlock, job: "terminate"},
		{failurePolicy: PreMitigationJobFailureContinue},
	}
	for _, want := range tests {
		encoded := want.String()
		got, err := parsePlan(encoded)
		if err != nil {
			t.Fatalf("parsePlan(%q): %v", encoded, err)
		}
		if got != want {
			t.Errorf("round trip %q = %#v, want %#v", encoded, got, want)
		}
	}
}

func TestPlanParseRejectsUnknownFailurePolicy(t *testing.T) {
	if _, err := parsePlan("failurePolicy=Retry"); err == nil {
		t.Fatal("parsePlan accepted an unknown failure policy")
	}
}

func TestPlanTakeover(t *testing.T) {
	base := plan{taint: true, drainTimeout: drainNever}
	tests := []struct {
		name       string
		owner      string
		plan       plan
		candidates []string
		conditions map[string]ConditionConfig
		wantOwner  string
		wantPlan   plan
	}{
		{
			name:       "condition without preMitigationJob leaves block unchanged",
			owner:      "owner",
			plan:       plan{taint: true, drain: true, drainTimeout: drainNever, failurePolicy: PreMitigationJobFailureBlock},
			candidates: []string{"other"},
			conditions: map[string]ConditionConfig{"other": {}},
			wantOwner:  "owner",
			wantPlan:   plan{taint: true, drain: true, drainTimeout: drainNever, failurePolicy: PreMitigationJobFailureBlock},
		},
		{
			name:       "a pre-mitigation Job candidate adds Block",
			owner:      "owner",
			plan:       plan{taint: true, drain: true, drainTimeout: drainNever},
			candidates: []string{"other"},
			conditions: map[string]ConditionConfig{"other": {PreMitigationJob: JobRef{Name: "diagnostics", FailurePolicy: PreMitigationJobFailureBlock}}},
			wantOwner:  "other",
			wantPlan:   plan{taint: true, drain: true, drainTimeout: drainNever, failurePolicy: PreMitigationJobFailureBlock},
		},

		{
			name:       "shorter drain timeout takes ownership",
			owner:      "owner",
			plan:       plan{taint: true, drain: true, drainTimeout: 30 * time.Minute},
			candidates: []string{"other"},
			conditions: map[string]ConditionConfig{"other": {DrainTimeout: "5m"}},
			wantOwner:  "other",
			wantPlan:   plan{taint: true, drain: true, drainTimeout: 5 * time.Minute},
		},
		{
			name:       "first mitigation Job remains selected",
			owner:      "owner",
			plan:       plan{drainTimeout: drainNever, job: "first"},
			candidates: []string{"other"},
			conditions: map[string]ConditionConfig{"other": {Taint: new(false), Drain: new(false), MitigationJob: JobRef{Name: "second"}}},
			wantOwner:  "owner",
			wantPlan:   plan{drainTimeout: drainNever, job: "first"},
		},
		{
			name:       "non-aggressive condition does not take over",
			owner:      "owner",
			plan:       base,
			candidates: []string{"other"},
			conditions: map[string]ConditionConfig{"other": {Taint: new(false)}},
			wantOwner:  "owner",
			wantPlan:   base,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{Conditions: tt.conditions}
			gotOwner, gotPlan := takeover(tt.owner, tt.plan, tt.candidates, cfg)
			if gotOwner != tt.wantOwner || gotPlan != tt.wantPlan {
				t.Fatalf("takeover() = (%q, %#v), want (%q, %#v)", gotOwner, gotPlan, tt.wantOwner, tt.wantPlan)
			}
		})
	}
}

func TestPlanParseShortFormsAndDefaults(t *testing.T) {
	gentle := plan{taint: true, drain: true, drainTimeout: 30 * time.Minute, job: "terminate"}
	tests := map[string]plan{
		"": {taint: true, drain: true, drainTimeout: drainNever},
		"taint,drain,drainTimeout=30m,job=terminate":                                 gentle,
		"taint=true,drain,drainTimeout=30m,job=terminate":                            gentle,
		"taint=true,drain=true,drainTimeout=30m,skipPodDeletion=false,job=terminate": gentle,
		"drainTimeout=30m,job=terminate":                                             gentle,
		"taint=false":                                                                {drainTimeout: drainNever},
		"drain=false":                                                                {taint: true, drainTimeout: drainNever},
		"skipPodDeletion":                                                            {taint: true, drain: true, drainTimeout: drainNever, skipPodDeletion: true},
	}
	for encoded, want := range tests {
		got, err := parsePlan(encoded)
		if err != nil {
			t.Fatalf("parsePlan(%q): %v", encoded, err)
		}
		if got != want {
			t.Errorf("parsePlan(%q) = %#v, want %#v", encoded, got, want)
		}
	}
	for _, bad := range []string{"job", "drainTimeout", "failurePolicy", "taint=maybe"} {
		if _, err := parsePlan(bad); err == nil {
			t.Errorf("parsePlan(%q) accepted an invalid step", bad)
		}
	}
}
