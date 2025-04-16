{{/*
Expand the name of the chart.
*/}}
{{- define "unitycatalog.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "unitycatalog.fullname" -}}
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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "unitycatalog.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "unitycatalog.commonLabels" -}}
helm.sh/chart: {{ include "unitycatalog.chart" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
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
app.kubernetes.io/name: {{ include "unitycatalog.name" . }}-server
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Server full name
*/}}
{{- define "unitycatalog.server.fullname" -}}
{{- include "unitycatalog.fullname" . }}-server
{{- end }}

{{/*
Server API endpoint
*/}}
{{- define "unitycatalog.server.apiEndpoint" -}}
http://{{ include "unitycatalog.server.fullname" . }}:{{ .Values.server.service.port }}
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
app.kubernetes.io/name: {{ include "unitycatalog.name" . }}-ui
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
UI full name
*/}}
{{- define "unitycatalog.ui.fullname" -}}
{{- include "unitycatalog.fullname" . }}-ui
{{- end }}
