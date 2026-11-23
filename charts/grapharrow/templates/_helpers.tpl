{{- define "grapharrow.selector" -}}
app.kubernetes.io/name: grapharrow
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: server
{{- end -}}

{{- define "grapharrow.gatewaySelector" -}}
app.kubernetes.io/name: grapharrow
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: gateway
{{- end -}}

{{- define "grapharrow.securityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: [ALL]
{{- end -}}

{{- define "grapharrow.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
fsGroup: 65532
fsGroupChangePolicy: OnRootMismatch
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "grapharrow.image" -}}
{{- if .Values.image.digest -}}
{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- else -}}
{{ .Values.image.repository }}:{{ default .Chart.AppVersion .Values.image.tag }}
{{- end -}}
{{- end -}}

{{- define "grapharrow.ports" -}}
- name: http
  port: 8000
  targetPort: http
- name: bolt
  port: 7687
  targetPort: bolt
{{- end -}}

{{- define "grapharrow.spread" -}}
- maxSkew: 1
  topologyKey: kubernetes.io/hostname
  whenUnsatisfiable: ScheduleAnyway
  labelSelector:
    matchLabels:
      {{- . | nindent 6 }}
{{- end -}}

{{- define "grapharrow.gatewaySlots" -}}
{{- if .Values.autoscaling.enabled -}}
{{- .Values.autoscaling.maxReplicas -}}
{{- else -}}
{{- .Values.replicaCount -}}
{{- end -}}
{{- end -}}

{{- define "grapharrow.useGateway" -}}
{{- if or (eq .Values.storage.backend "delta") .Values.storage.readOnly -}}
true
{{- end -}}
{{- end -}}

{{- define "grapharrow.validate" -}}
{{- if .Values.autoscaling.enabled -}}
  {{- if not (include "grapharrow.useGateway" .) -}}
    {{- fail "Autoscaling requires Delta or read-only Native replicas" -}}
  {{- end -}}
  {{- if gt (int .Values.autoscaling.minReplicas) (int .Values.autoscaling.maxReplicas) -}}
    {{- fail "autoscaling.minReplicas must be less than or equal to autoscaling.maxReplicas" -}}
  {{- end -}}
  {{- range $container := list "http" "bolt" -}}
    {{- if not (dig $container "requests" "cpu" "" $.Values.resources) -}}
      {{- fail (printf "Autoscaling requires resources.%s.requests.cpu" $container) -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
{{- if eq .Values.storage.backend "native" -}}
  {{- if and (not .Values.storage.readOnly) (ne (int .Values.replicaCount) 1) -}}
    {{- fail "A writable Native release requires exactly one GraphArrow replica" -}}
  {{- end -}}
  {{- if or .Values.storage.uri .Values.storage.existingSecret .Values.storage.readOnly -}}
    {{- if not (regexMatch "^(s3|az)://[^/[:space:]]+/[^[:space:]]+$" .Values.storage.uri) -}}
      {{- fail "Native object storage requires a shared s3://bucket/prefix or az://container/prefix database URI" -}}
    {{- end -}}
    {{- if not .Values.storage.existingSecret -}}
      {{- fail "Native object storage requires storage.existingSecret" -}}
    {{- end -}}
    {{- if .Values.storage.pvc.existingClaim -}}
      {{- fail "Native object storage uses storage.uri instead of storage.pvc.existingClaim" -}}
    {{- end -}}
  {{- end -}}
{{- else if eq .Values.storage.backend "delta" -}}
  {{- if .Values.storage.readOnly -}}
    {{- fail "storage.readOnly requires the Native storage backend" -}}
  {{- end -}}
  {{- if not (regexMatch "^(s3|az)://[^/[:space:]]+/[^[:space:]]+$" .Values.storage.uri) -}}
    {{- fail "Delta storage requires a shared s3://bucket/prefix or az://container/prefix database URI" -}}
  {{- end -}}
  {{- if .Values.storage.pvc.existingClaim -}}
    {{- fail "Delta storage uses storage.uri; storage.pvc.existingClaim is for Native" -}}
  {{- end -}}
{{- else -}}
  {{- fail "storage.backend must be native or delta" -}}
{{- end -}}
{{- end -}}
