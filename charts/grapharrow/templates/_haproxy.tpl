{{- define "grapharrow.gatewayConfig" -}}
{{- $ordinals := until (include "grapharrow.gatewaySlots" . | int) -}}
{{- $name := .Release.Name -}}
global
    stats socket /run/haproxy/admin.sock mode 600 level admin

defaults
    timeout connect 5s
    timeout client 2m
    timeout server 2m

frontend http
    bind :8000
    mode http
    acl workflow path /tools/call /tools/call/ /sessions/current /sessions/current/
    acl session_header req.fhdr_cnt(X-ANA-Session) gt 0
    acl multiple_session_headers req.fhdr_cnt(X-ANA-Session) gt 1
{{- range $ordinals }}
    acl replica_{{ . }} req.fhdr(X-ANA-Session) -m beg {{ $name }}-{{ . }}.
    acl unavailable_{{ . }} nbsrv(http_{{ . }}) lt 1
{{- end }}
    http-request return status 404 content-type application/json string '{"detail":"Unknown or expired session"}' hdr Cache-Control no-store if workflow multiple_session_headers
    http-request return status 404 content-type application/json string '{"detail":"Unknown or expired session"}' hdr Cache-Control no-store if workflow session_header{{ range $ordinals }} !replica_{{ . }}{{ end }}
{{- range $ordinals }}
    http-request return status 404 content-type application/json string '{"detail":"Unknown or expired session"}' hdr Cache-Control no-store if workflow replica_{{ . }} unavailable_{{ . }}
{{- end }}
{{- range $ordinals }}
    use_backend http_{{ . }} if workflow replica_{{ . }}
{{- end }}
    default_backend http_pool

backend http_pool
    mode http
    balance roundrobin
    option httpchk GET /health
    retries 0
{{- range $ordinals }}
    server app_{{ . }} {{ $name }}-pod-{{ . }}:8000 check inter 1s init-state down
{{- end }}
{{- range $ordinals }}

backend http_{{ . }}
    mode http
    retries 0
    # Route an issued session using the same health state that admitted it.
    server app_{{ . }} {{ $name }}-pod-{{ . }}:8000 track http_pool/app_{{ . }}
{{- end }}

listen bolt
    bind :7687
    mode tcp
    timeout client 1h
    timeout server 1h
    balance roundrobin
    retries 0
{{- range $ordinals }}
    server app_{{ . }} {{ $name }}-pod-{{ . }}:7687 check inter 1s init-state down
{{- end }}
{{- end -}}
