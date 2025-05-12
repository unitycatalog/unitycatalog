{{/*
Expand the name of the chart.
*/}}
{{- define "unitycatalog.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 56 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 56 chars for all fullnames depending on this to fit 63 characters (longest suffix "-server").
Some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "unitycatalog.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 56 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 56 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 56 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "unitycatalog.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 56 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "unitycatalog.commonLabels" -}}
helm.sh/chart: {{ include "unitycatalog.chart" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "unitycatalog.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "unitycatalog.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Server definitions
*/}}

{{/*
Server labels
*/}}
{{- define "unitycatalog.server.labels" -}}
{{ include "unitycatalog.commonLabels" . }}
{{ include "unitycatalog.server.selectorLabels" . }}
{{- end }}

{{/*
Server selector labels
*/}}
{{- define "unitycatalog.server.selectorLabels" -}}
app.kubernetes.io/name: "unitycatalog"
app.kubernetes.io/component: "server"
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end }}

{{/*
Server full name
*/}}
{{- define "unitycatalog.server.fullname" -}}
{{- include "unitycatalog.fullname" . }}-server
{{- end }}

{{/*
UI definitions
*/}}

{{/*
UI labels
*/}}
{{- define "unitycatalog.ui.labels" -}}
{{ include "unitycatalog.commonLabels" . }}
{{ include "unitycatalog.ui.selectorLabels" . }}
{{- end }}

{{/*
UI selector labels
*/}}
{{- define "unitycatalog.ui.selectorLabels" -}}
app.kubernetes.io/name: "unitycatalog"
app.kubernetes.io/component: "ui"
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end }}

{{/*
UI full name
*/}}
{{- define "unitycatalog.ui.fullname" -}}
{{- include "unitycatalog.fullname" . }}-ui
{{- end }}

{{/*
File DB PVC name}}
*/}}
{{- define "unitycatalog.server.db.filePvcName" -}}
{{- default (printf "%s-%s" (include "unitycatalog.server.fullname" .) "db") .Values.server.db.fileConfig.persistence.pvcName }}
{{- end }}

{{/*
Keypair secret name
*/}}
{{- define "unitycatalog.server.jwtKeypairSecretName" -}}
{{- default (printf "%s-%s" (include "unitycatalog.server.fullname" .) "jwt-key") .Values.server.jwtKeypairSecret.name }}
{{- end }}

{{/*
Enable UI API proxy
*/}}
{{- define "unitycatalog.ui.proxyApiRequests" -}}
{{- if quote .Values.ui.proxyApiRequests | empty }}
    {{- not (or .Values.ingress.enabled .Values.httpRoute.enabled) }}
{{- else if .Values.ui.proxyApiRequests }}
    {{- if or .Values.ingress.enabled .Values.httpRoute.enabled }}
    {{- fail "Proxying API requests must be disabled when using httpRoute or ingress" }}
    {{- end -}}
    true
{{- else -}}
    false
{{- end }}
{{- end }}
