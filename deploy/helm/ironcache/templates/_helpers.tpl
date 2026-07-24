{{- /* SPDX-License-Identifier: MIT OR Apache-2.0 */ -}}

{{/* Chart name (overridable). */}}
{{- define "ironcache.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully-qualified release name (the StatefulSet / Service base name). */}}
{{- define "ironcache.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "ironcache.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/* The headless Service name (stable per-pod DNS). */}}
{{- define "ironcache.headlessService" -}}
{{- printf "%s-headless" (include "ironcache.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Common labels. */}}
{{- define "ironcache.labels" -}}
app.kubernetes.io/name: {{ include "ironcache.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{/* Selector labels (stable across upgrades; do NOT add version here). */}}
{{- define "ironcache.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ironcache.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
A stable 40-lowercase-hex cluster_announce_id for the pod at a given ordinal.
The id must be 40 hex chars (the node-id contract) and STABLE across reboots, so
we derive it deterministically from the fullname + ordinal via sha256 (hex) and
take the first 40 chars. The init container computes the SAME value at runtime
(it has the ordinal from its hostname), so the topology entry and the pod agree.
Usage: include "ironcache.nodeId" (dict "ctx" . "ordinal" 0)
*/}}
{{- define "ironcache.nodeId" -}}
{{- $seed := printf "%s-%d" (include "ironcache.fullname" .ctx) (int .ordinal) -}}
{{- substr 0 40 (sha256sum $seed) -}}
{{- end -}}

{{/* The console workload / Service base name (a distinct resource from the StatefulSet). */}}
{{- define "ironcache.console.fullname" -}}
{{- printf "%s-console" (include "ironcache.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Console selector labels. CRITICAL: the console uses a DISTINCT
`app.kubernetes.io/name` (`<name>-console`), not the cache's `<name>`, so the
console pod labels are NOT a superset of the cache selector labels. If the console
reused the cache name, the cache PDB/Services/StatefulSet selectors (which are just
{name, instance}) would ALSO select the console pods -- a cross-controller PDB that
can block a cache node drain. A distinct name avoids that WITHOUT touching (and
breaking) the cache's immutable StatefulSet selector. Version is omitted (stable
across upgrades), matching ironcache.selectorLabels.
*/}}
{{- define "ironcache.console.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ironcache.name" . }}-console
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: console
{{- end -}}

{{/* Console labels: the selector labels PLUS the managed-by / chart / version metadata. */}}
{{- define "ironcache.console.labels" -}}
{{ include "ironcache.console.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{/*
The AUTO-GENERATED cluster_secret, made STABLE across `helm upgrade`: reuse the value already
stored in THIS release's Secret (via `lookup`) instead of minting a fresh random on every render,
so a bare `helm upgrade` does NOT rotate the peer secret and split the cluster. A fresh random is
minted only on the FIRST install (no prior Secret). IMPORTANT: `lookup` returns empty under
`helm template` / `--dry-run` / a GitOps templater without live cluster access, so THOSE paths
regenerate every time -- for GitOps (Argo/Flux) or any reproducible pipeline you MUST set an
explicit clusterSecret.value or clusterSecret.existingSecret (source-controlled, never rotates).
*/}}
{{- define "ironcache.clusterSecretAuto" -}}
{{- $name := printf "%s-secret" (include "ironcache.fullname" .) -}}
{{- $prior := lookup "v1" "Secret" .Release.Namespace $name -}}
{{- $existing := "" -}}
{{- /* Guard `.data`: an externally pre-created Secret at this name (a GitOps / external-secrets
       placeholder) can be a truthy object with a NIL `data`, and `index nil ...` PANICS the whole
       render -- `| default` does not rescue it (index fails first). `and $prior $prior.data` renders
       a fresh random for a data-less prior instead of exploding. */ -}}
{{- if and $prior $prior.data -}}
{{- $existing = (index $prior.data "cluster_secret" | default "") -}}
{{- end -}}
{{- if $existing -}}
{{- $existing | b64dec -}}
{{- else -}}
{{- randAlphaNum 40 -}}
{{- end -}}
{{- end -}}
