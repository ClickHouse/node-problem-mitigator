{{/* Expand the chart name. */}}
{{- define "node-problem-mitigator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Create a fully qualified app name. */}}
{{- define "node-problem-mitigator.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/* Create the chart label value. */}}
{{- define "node-problem-mitigator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | trunc 63 | trimAll "-_." }}
{{- end }}

{{/* Common labels. */}}
{{- define "node-problem-mitigator.labels" -}}
helm.sh/chart: {{ include "node-problem-mitigator.chart" . }}
{{ include "node-problem-mitigator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | trunc 63 | trimAll "-_." | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* Selector labels. */}}
{{- define "node-problem-mitigator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "node-problem-mitigator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Select or create the controller ServiceAccount name. */}}
{{- define "node-problem-mitigator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "node-problem-mitigator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* Render an administrator-controlled value as a Helm template. */}}
{{- define "node-problem-mitigator.tpl" -}}
{{- tpl (toString .value) .context -}}
{{- end }}

{{/*
Normalize a Job ServiceAccount block. Job names provide the default name when one is omitted.
*/}}
{{- define "node-problem-mitigator.jobServiceAccount" -}}
{{- $block := .block | default dict -}}
{{- $create := true -}}
{{- if hasKey $block "create" }}{{ $create = $block.create }}{{ end -}}
{{- $name := toString ($block.name | default "") -}}
{{- if and (not $name) $create }}{{ $name = .defaultName }}{{ end -}}
{{- if not $name }}{{ fail (printf "%s: serviceAccount.name is required" .owner) }}{{ end -}}
{{- if not (and (le (len $name) 253) (regexMatch `^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$` $name)) }}
{{- fail (printf "%s: ServiceAccount name %q must be a lowercase RFC 1123 subdomain of at most 253 characters" .owner $name) }}
{{- end -}}
{{- dict "name" $name "create" $create "annotations" ($block.annotations | default dict) "rules" ($block.rules | default list) "clusterRules" ($block.clusterRules | default list) | toYaml -}}
{{- end }}

{{/* Collect each declared Job ServiceAccount once, including its RBAC rules. */}}
{{- define "node-problem-mitigator.jobServiceAccounts" -}}
{{- $fullname := include "node-problem-mitigator.fullname" . -}}
{{- $entries := list -}}
{{- range $name, $job := .Values.jobs -}}
{{- if not (kindIs "invalid" $job) -}}
{{- if kindIs "map" $job.serviceAccount -}}
{{- $entries = append $entries (dict "owner" (printf "Job %q" $name) "block" $job.serviceAccount "defaultName" (printf "%s-%s" $fullname $name)) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $controller := include "node-problem-mitigator.serviceAccountName" . -}}
{{- $accounts := dict -}}
{{- $owners := dict -}}
{{- range $entry := $entries -}}
{{- $account := include "node-problem-mitigator.jobServiceAccount" $entry | fromYaml -}}
{{- $name := $account.name -}}
{{- if eq $name $controller -}}
{{- fail (printf "%s: ServiceAccount %q is the controller ServiceAccount" $entry.owner $name) -}}
{{- end -}}
{{- $spec := omit $account "name" -}}
{{- if hasKey $accounts $name -}}
{{- if ne (toYaml (get $accounts $name)) (toYaml $spec) -}}
{{- fail (printf "ServiceAccount %q is declared differently by %s and %s" $name (get $owners $name) $entry.owner) -}}
{{- end -}}
{{- else -}}
{{- $_ := set $accounts $name $spec -}}
{{- $_ := set $owners $name $entry.owner -}}
{{- end -}}
{{- end -}}
{{- toYaml $accounts -}}
{{- end }}

{{/*
Normalize a Job template for the ConfigMap. A serviceAccount block controls the pod account.
*/}}
{{- define "node-problem-mitigator.jobTemplate" -}}
{{- $template := deepCopy (required (printf "%s.template is required" .path) .template) -}}
{{- $jobSpec := $template.spec | default dict -}}
{{- $_ := set $template "spec" $jobSpec -}}
{{- $podTemplate := $jobSpec.template | default dict -}}
{{- $_ := set $jobSpec "template" $podTemplate -}}
{{- $podSpec := $podTemplate.spec | default dict -}}
{{- $_ := set $podTemplate "spec" $podSpec -}}
{{- if not $podSpec.containers -}}
{{- fail (printf "%s.template.spec.template.spec.containers is required" .path) -}}
{{- end -}}
{{- if hasKey $jobSpec "ttlSecondsAfterFinished" -}}
{{- fail (printf "%s.template.spec.ttlSecondsAfterFinished is not supported; the controller deletes finished Jobs after app.config.jobRetention" .path) -}}
{{- end -}}
{{- if kindIs "map" .serviceAccount -}}
{{- $account := include "node-problem-mitigator.jobServiceAccount" (dict "owner" .owner "block" .serviceAccount "defaultName" .defaultName) | fromYaml -}}
{{- $current := toString ($podSpec.serviceAccountName | default "") -}}
{{- if and $current (ne $current $account.name) -}}
{{- fail (printf "%s: template serviceAccountName %q differs from serviceAccount.name %q" .owner $current $account.name) -}}
{{- end -}}
{{- $_ := set $podSpec "serviceAccountName" $account.name -}}
{{- end -}}
{{- toYaml $template -}}
{{- end }}

{{/* Validate condition Job references. */}}
{{- define "node-problem-mitigator.validateJobs" -}}
{{- $conditions := dict -}}
{{- range $conditionName, $condition := .Values.app.config.conditions -}}
{{- if not (kindIs "invalid" $condition) -}}{{- $_ := set $conditions $conditionName $condition -}}{{- end -}}
{{- end -}}
{{- range $conditionName, $condition := $conditions -}}
{{- $taints := ne (toString (dig "taint" true ($condition | default dict))) "false" -}}
{{- if hasKey ($condition | default dict) "preMitigationJob" -}}
{{- $reference := $condition.preMitigationJob -}}
{{- if not (kindIs "map" $reference) -}}
{{- fail (printf "app.config.conditions.%s.preMitigationJob must be a map with a name" $conditionName) -}}
{{- end -}}
{{- if not (hasKey $reference "name") -}}
{{- fail (printf "app.config.conditions.%s.preMitigationJob.name is required" $conditionName) -}}
{{- end -}}
{{- $name := toString $reference.name -}}
{{- $jobs := $.Values.jobs | default dict -}}
{{- if or (not (hasKey $jobs $name)) (kindIs "invalid" (get $jobs $name)) -}}
{{- fail (printf "app.config.conditions.%s.preMitigationJob.name %q does not match a Job in top-level jobs" $conditionName $name) -}}
{{- end -}}
{{- if hasKey $reference "failurePolicy" -}}
{{- if not (has $reference.failurePolicy (list "Continue" "Block")) -}}
{{- fail (printf "app.config.conditions.%s.preMitigationJob.failurePolicy must be Continue or Block, got %q" $conditionName (toString $reference.failurePolicy)) -}}
{{- end -}}
{{- end -}}
{{- if and (not $taints) (eq (toString $reference.failurePolicy) "Block") -}}
{{- fail (printf "app.config.conditions.%s.preMitigationJob.failurePolicy Block requires taint" $conditionName) -}}
{{- end -}}
{{- end -}}
{{- if hasKey ($condition | default dict) "mitigationJob" -}}
{{- $reference := $condition.mitigationJob -}}
{{- if not (kindIs "map" $reference) -}}
{{- fail (printf "app.config.conditions.%s.mitigationJob must be a map with a name" $conditionName) -}}
{{- end -}}
{{- if not (hasKey $reference "name") -}}
{{- fail (printf "app.config.conditions.%s.mitigationJob.name is required" $conditionName) -}}
{{- end -}}
{{- $name := toString $reference.name -}}
{{- $jobs := $.Values.jobs | default dict -}}
{{- if or (not (hasKey $jobs $name)) (kindIs "invalid" (get $jobs $name)) -}}
{{- fail (printf "app.config.conditions.%s.mitigationJob.name %q does not match a Job in top-level jobs" $conditionName $name) -}}
{{- end -}}
{{- if hasKey $reference "failurePolicy" -}}
{{- fail (printf "app.config.conditions.%s.mitigationJob cannot set failurePolicy" $conditionName) -}}
{{- end -}}
{{- if not $taints -}}
{{- fail (printf "app.config.conditions.%s.mitigationJob requires taint" $conditionName) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/* Build the controller's combined mitigation and pre-mitigation Job config. */}}
{{- define "node-problem-mitigator.jobs" -}}
{{- include "node-problem-mitigator.validateJobs" . -}}
{{- $fullname := include "node-problem-mitigator.fullname" . -}}
{{- $jobs := dict -}}
{{- range $name, $job := .Values.jobs -}}
{{- if not (kindIs "invalid" $job) -}}
{{- $path := printf "jobs.%s" $name -}}
{{- $owner := printf "Job %q" $name -}}
{{- $template := include "node-problem-mitigator.jobTemplate" (dict "path" $path "owner" $owner "template" $job.template "serviceAccount" $job.serviceAccount "defaultName" (printf "%s-%s" $fullname $name)) | fromYaml -}}
{{- $_ := set $jobs $name (dict "template" $template) -}}
{{- end -}}
{{- end -}}
{{- toYaml $jobs -}}
{{- end }}

{{/*
Render one alerting rule. Labels: shared labels, then severity, then per-alert labels.
Annotations: summary, description, dashboard_url, then per-alert annotations.
*/}}
{{- define "node-problem-mitigator.alertRule" -}}
{{- $root := .root -}}
{{- $settings := .settings -}}
{{- $merged := deepCopy (.prometheusRule.labels | default dict) -}}
{{- with $settings.severity }}{{ $_ := set $merged "severity" . }}{{ end -}}
{{- $merged = mergeOverwrite $merged (deepCopy ($settings.labels | default dict)) -}}
{{- $annotations := dict "summary" .summary "description" .description -}}
{{- with .prometheusRule.dashboardUrl }}{{ $_ := set $annotations "dashboard_url" . }}{{ end -}}
{{- $annotations = mergeOverwrite $annotations (deepCopy ($settings.annotations | default dict)) -}}
{{- $rule := dict "alert" .name "expr" .expr -}}
{{- with $settings.for }}{{ $_ := set $rule "for" (toString .) }}{{ end -}}
{{- $labels := dict -}}
{{- range $key, $value := $merged }}{{ if not (kindIs "invalid" $value) }}{{ $_ := set $labels $key (tpl (toString $value) $root) }}{{ end }}{{ end -}}
{{- with $labels }}{{ $_ := set $rule "labels" . }}{{ end -}}
{{- $strings := dict -}}
{{- range $key, $value := $annotations }}{{ $_ := set $strings $key (toString $value) }}{{ end -}}
{{- $_ := set $rule "annotations" $strings -}}
{{- toYaml $rule -}}
{{- end }}
