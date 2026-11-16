package controller

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// seriesValue returns the counter value or histogram sample count of the
// series with exactly these labels, and false if the series does not exist.
func seriesValue(t *testing.T, c prometheus.Collector, labels map[string]string) (float64, bool) {
	t.Helper()
	ch := make(chan prometheus.Metric)
	go func() { c.Collect(ch); close(ch) }()
	var value float64
	found := false
	for m := range ch {
		var metric dto.Metric
		if err := m.Write(&metric); err != nil {
			t.Fatal(err)
		}
		if found || len(metric.GetLabel()) != len(labels) {
			continue
		}
		match := true
		for _, pair := range metric.GetLabel() {
			if labels[pair.GetName()] != pair.GetValue() {
				match = false
			}
		}
		if match {
			found = true
			value = metric.GetCounter().GetValue() + float64(metric.GetHistogram().GetSampleCount())
		}
	}
	return value, found
}

func TestSeedMetrics(t *testing.T) {
	cfg := Config{Conditions: map[string]ConditionConfig{
		"SeedCheck": {
			PreMitigationJob: JobRef{Name: "seed-diagnostics"},
			MitigationJob:    JobRef{Name: "seed-terminate"},
		},
	}}
	series := []struct {
		name   string
		metric prometheus.Collector
		labels map[string]string
	}{
		{"action duration", mitigationActionDuration, map[string]string{labelCondition: "SeedCheck"}},
		{"total duration", mitigationTotalDuration, map[string]string{labelCondition: "SeedCheck"}},
		{"recovery time", conditionRecoveryTime, map[string]string{labelCondition: "SeedCheck"}},
		{"nodes mitigated", nodeMitigatedTotal, map[string]string{labelCondition: "SeedCheck", labelTainted: "true"}},
		{"pre-mitigation Job duration", jobDuration, map[string]string{labelJobKind: jobKindPreMitigation, labelJob: "seed-diagnostics"}},
		{"mitigation Job duration", jobDuration, map[string]string{labelJobKind: jobKindMitigation, labelJob: "seed-terminate"}},
	}
	for _, s := range series {
		if _, found := seriesValue(t, s.metric, s.labels); found {
			t.Fatalf("%s series exists before seeding", s.name)
		}
	}

	seedMetrics(cfg)

	for _, s := range series {
		value, found := seriesValue(t, s.metric, s.labels)
		if !found {
			t.Errorf("%s series was not seeded", s.name)
		} else if value != 0 {
			t.Errorf("%s series seeded at %v, want 0", s.name, value)
		}
	}
}
