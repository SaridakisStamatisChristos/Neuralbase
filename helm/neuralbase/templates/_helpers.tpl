{{/*
NeuralBase Helm chart helpers.
*/}}

{{- define "neuralbase.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "neuralbase.fullname" -}}
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

{{- define "neuralbase.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "neuralbase.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "neuralbase.selectorLabels" -}}
app.kubernetes.io/name: {{ include "neuralbase.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Build a stable logical-id -> DNS:port map for every StatefulSet replica.
Example for 3 replicas:
release-neuralbase-0=release-neuralbase-0.release-neuralbase-headless:7001,...
*/}}
{{- define "neuralbase.raftPeers" -}}
{{- $fullname := include "neuralbase.fullname" . -}}
{{- $peers := list -}}
{{- range $i := until (int .Values.replicaCount) -}}
{{- $id := printf "%s-%d" $fullname $i -}}
{{- $addr := printf "%s.%s-headless:7001" $id $fullname -}}
{{- $peers = append $peers (printf "%s=%s" $id $addr) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end }}
