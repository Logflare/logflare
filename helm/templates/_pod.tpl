{{/*
Pod template shared by the Deployment and the Rollout, so the two controllers can never
drift apart. Rendered with `nindent 4` under a `template:` key.
*/}}
{{- define "logflare.podTemplate" -}}
metadata:
  annotations:
    checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}
    {{- with .Values.podAnnotations }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  labels:
    {{- include "logflare.labels" . | nindent 4 }}
    {{- with .Values.podLabels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
spec:
  {{- with .Values.imagePullSecrets }}
  imagePullSecrets:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  serviceAccountName: {{ include "logflare.serviceAccountName" . }}
  {{- with .Values.terminationGracePeriodSeconds }}
  terminationGracePeriodSeconds: {{ . }}
  {{- end }}
  {{- with .Values.podSecurityContext }}
  securityContext:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.initContainers }}
  initContainers:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  containers:
    - name: {{ .Chart.Name }}
      {{- with .Values.securityContext }}
      securityContext:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
      imagePullPolicy: {{ .Values.image.pullPolicy }}
      ports:
        - name: http
          containerPort: {{ .Values.service.port }}
          protocol: TCP
        - name: grpc
          containerPort: {{ .Values.logflare.grpcPort }}
          protocol: TCP
      env:
        {{- if .Values.logflare.nodeHost }}
        - name: LOGFLARE_NODE_HOST
          value: {{ .Values.logflare.nodeHost | quote }}
        {{- else }}
        - name: LOGFLARE_NODE_HOST
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        {{- end }}
        {{- if .Values.logflare.certFilesSecret }}
        {{- $mount := .Values.logflare.certFilesMountPath }}
        - name: DB_SSL_CA_CERT_PATH
          value: {{ printf "%s/db-server-ca.pem" $mount | quote }}
        - name: DB_SSL_CLIENT_CERT_PATH
          value: {{ printf "%s/db-client-cert.pem" $mount | quote }}
        - name: DB_SSL_CLIENT_KEY_PATH
          value: {{ printf "%s/db-client-key.pem" $mount | quote }}
        - name: LOGFLARE_TLS_CERT_PATH
          value: {{ printf "%s/cert.pem" $mount | quote }}
        - name: LOGFLARE_TLS_KEY_PATH
          value: {{ printf "%s/cert.key" $mount | quote }}
        {{- end }}
        {{- /* The appVersion image predates these flags in rel/vm.args.eex. Drop them once it ships them. */}}
        - name: ERL_FLAGS
          value: {{ printf "+Q 1048576 +sbwt none +sbwtdcpu none +sbwtdio none %s" (dig "ERL_FLAGS" "" (.Values.logflare.extraConfig | default dict)) | trim | quote }}
        {{- with .Values.extraEnv }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      envFrom:
        - configMapRef:
            name: {{ include "logflare.fullname" . }}
        {{- range .Values.logflare.secretRefs }}
        - secretRef:
            name: {{ . }}
        {{- end }}
      {{- with .Values.livenessProbe }}
      livenessProbe:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.readinessProbe }}
      readinessProbe:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.startupProbe }}
      startupProbe:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.resources }}
      resources:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- if or .Values.logflare.certFilesSecret .Values.volumeMounts }}
      volumeMounts:
        {{- if .Values.logflare.certFilesSecret }}
        - name: cert-files
          mountPath: {{ .Values.logflare.certFilesMountPath }}
          readOnly: true
        {{- end }}
        {{- with .Values.volumeMounts }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      {{- end }}
  {{- if or .Values.logflare.certFilesSecret .Values.volumes }}
  volumes:
    {{- if .Values.logflare.certFilesSecret }}
    - name: cert-files
      secret:
        secretName: {{ .Values.logflare.certFilesSecret }}
    {{- end }}
    {{- with .Values.volumes }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- end }}
  {{- with .Values.nodeSelector }}
  nodeSelector:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.affinity }}
  affinity:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.tolerations }}
  tolerations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.topologySpreadConstraints }}
  topologySpreadConstraints:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end }}
