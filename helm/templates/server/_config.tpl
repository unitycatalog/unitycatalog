{{- define "unitycatalog.server.configTemplate" -}}

{{- if .Values.server.config.override -}}
{{ .Values.server.config.override }}
{{- else -}}
server.env=prod
{{- if .Values.auth.enabled }}
server.authorization=enable
server.authorization-url={{ .Values.auth.authorizationUrl }}
server.token-url={{ .Values.auth.tokenUrl }}
server.client-id=${OAUTH_CLIENT_ID}
server.redirect-port={{ .Values.auth.redirectPort }}
server.cookie-timeout={{ .Values.auth.cookieTimeout }}
server.access-token-timeout={{ .Values.auth.accessTokenTimeout }}
server.allowed-issuers={{ .Values.auth.allowedIssuers }}
server.allowed-jwks-uris={{ .Values.auth.allowedJwksUris }}
server.audiences={{ .Values.auth.audiences }}
{{- end }}

storage-root.models={{ .Values.storage.modelStorageRoot }}

{{- range $index, $config := .Values.storage.credentials.s3 }}
s3.bucketPath.{{ $index }}={{ $config.bucketPath }}
s3.region.{{ $index }}={{ $config.region }}
s3.awsRoleArn.{{ $index }}={{ $config.awsRoleArn }}
{{- end }}

{{- range $index, $config := .Values.storage.credentials.adls }}
adls.storageAccountName.{{ $index }}={{ $config.storageAccountName }}
{{- end }}

{{- range $index, $config := .Values.storage.credentials.gcs }}
gcs.bucketPath.{{ $index }}={{ $config.bucketPath }}
gcs.jsonKeyFilePath.{{ $index }}=/etc/conf/gcs-credentials-{{ $index }}.json
{{- end }}
{{- end }}

{{- range $k, $v := .Values.server.config.extraProperties }}
{{ $k }}={{ $v }}
{{- end }}

{{- end }}

{{- define "unitycatalog.server.hibernateConfigTemplate" -}}
{{- if .Values.server.db.overrideConfig -}}
{{ .Values.server.db.overrideConfig }}
{{- else if eq .Values.server.db.type "file" -}}
hibernate.connection.driver_class=org.h2.Driver
hibernate.connection.url=jdbc:h2:file:./etc/db/h2db;DB_CLOSE_ON_EXIT=FALSE;DB_CLOSE_DELAY=0
{{- else if eq .Values.server.db.type "postgresql" -}}

{{- $params := dict }}

{{- if .Values.server.db.postgresqlConfig.ssl.enabled }}
{{- $params = merge $params (dict "sslmode" .Values.server.db.postgresqlConfig.ssl.mode) }}
{{- if or .Values.server.db.postgresqlConfig.ssl.rootCert .Values.server.db.postgresqlConfig.ssl.rootCertConfigMapName }}
{{- $params = merge $params (dict "sslrootcert" "/home/unitycatalog/etc/conf/dbrootca.crt") }}
{{- end }}
{{- end }}

{{- if .Values.server.db.postgresqlConfig.extraParams }}
{{- $params = merge $params (dict "extraParams" .Values.server.db.postgresqlConfig.extraParams) }}
{{- end }}

{{- $urlParamsList := list }}
{{- range $key, $value := $params }}
{{- $urlParamsList = append $urlParamsList (printf "%s=%s" $key $value) }}
{{- end }}

{{- $urlParams := "" }}
{{- if $urlParamsList }}
{{- $urlParams = printf "?%s" ($urlParamsList | join "&") }}
{{- end -}}

hibernate.connection.driver_class=org.postgresql.Driver
hibernate.connection.url=jdbc:postgresql://{{ .Values.server.db.postgresqlConfig.host }}:{{ .Values.server.db.postgresqlConfig.port }}/{{ .Values.server.db.postgresqlConfig.dbName }}{{ $urlParams }}
hibernate.connection.user={{ .Values.server.db.postgresqlConfig.username }}
{{- else if eq .Values.server.db.type "mysql" -}}

{{- $urlParamsList := list }}
{{- range $key, $value := .Values.server.db.mysqlConfig.extraParams }}
{{- $urlParamsList = append $urlParamsList (printf "%s=%s" (urlquery $key) (urlquery $value)) }}
{{- end }}

{{- $urlParams := "" }}
{{- if $urlParamsList }}
{{- $urlParams = printf "?%s" ($urlParamsList | join "&") }}
{{- end -}}

hibernate.connection.driver_class=com.mysql.cj.jdbc.Driver
{{- /* Connector/J form-decodes the whole URL, the database name too, so urlquery is the right escape for it. */}}
hibernate.connection.url=jdbc:mysql://{{ required "server.db.mysqlConfig.host is required" .Values.server.db.mysqlConfig.host }}:{{ .Values.server.db.mysqlConfig.port }}/{{ urlquery .Values.server.db.mysqlConfig.dbName }}{{ $urlParams }}
hibernate.connection.user={{ .Values.server.db.mysqlConfig.username }}
{{- else }}
{{ fail "Unsupported database type. Supported types are: file, postgresql, mysql." }}
{{- end }}

hibernate.hbm2ddl.auto=update
hibernate.show_sql=false
hibernate.archive.autodetection=class
hibernate.use_sql_comments=true
org.hibernate.SQL=INFO
org.hibernate.type.descriptor.sql.BasicBinder=TRACE
{{- end }}

{{- define "unitycatalog.server.log4j2ConfigTemplate" -}}
status=warn
appenders=console,auditFile

appender.console.type=Console
appender.console.name=Console
appender.console.layout.type=PatternLayout
appender.console.layout.pattern=%d{HH:mm:ss.SSS} ${hostName} [%t] %-5level %logger{36} %notEmpty{request_id=%X{request_id} }- %msg%n

# Audit records (server.audit.sink=io.unitycatalog.server.audit.FileAuditSink); no file is created
# until the first record. Rolls over every hour and at 10MB, and keeps up to 7 days or 1GB of archives.
appender.auditFile.type=RollingFile
appender.auditFile.name=AuditFile
appender.auditFile.fileName=/home/unitycatalog/etc/logs/audit.log
appender.auditFile.filePattern=/home/unitycatalog/etc/logs/audit-%d{yyyy-MM-dd-HH}-%i.log.gz
appender.auditFile.createOnDemand=true
appender.auditFile.ignoreExceptions=false
appender.auditFile.layout.type=PatternLayout
appender.auditFile.layout.pattern=%m%n
appender.auditFile.policies.type=Policies
appender.auditFile.policies.time.type=TimeBasedTriggeringPolicy
appender.auditFile.policies.time.interval=1
appender.auditFile.policies.size.type=SizeBasedTriggeringPolicy
appender.auditFile.policies.size.size=10MB
appender.auditFile.strategy.type=DefaultRolloverStrategy
appender.auditFile.strategy.max=20
appender.auditFile.strategy.fileIndex=max
appender.auditFile.strategy.action.type=Delete
appender.auditFile.strategy.action.basePath=/home/unitycatalog/etc/logs
appender.auditFile.strategy.action.condition.type=IfFileName
appender.auditFile.strategy.action.condition.glob=audit-*.log.gz
appender.auditFile.strategy.action.condition.any.type=IfAny
appender.auditFile.strategy.action.condition.any.age.type=IfLastModified
appender.auditFile.strategy.action.condition.any.age.age=P7D
appender.auditFile.strategy.action.condition.any.size.type=IfAccumulatedFileSize
appender.auditFile.strategy.action.condition.any.size.exceeds=1GB

logger.audit.name=audit
logger.audit.level=info
logger.audit.additivity=false
logger.audit.appenderRef.auditFile.ref=AuditFile

rootLogger.level={{ .Values.server.logLevel }}
rootLogger.appenderRefs=console
rootLogger.appenderRef.console.ref=Console
{{- end }}

{{/*
Secrets for the server container (see "Secrets" in docs/server/configuration.md): the database
password and every part of each storage credential come from here, not from the generated properties
files, which are written only at pod start.
*/}}
{{- define "unitycatalog.server.secretEnv" -}}
{{- $db := dict "postgresql" .Values.server.db.postgresqlConfig "mysql" .Values.server.db.mysqlConfig }}
{{- with get $db .Values.server.db.type }}
{{- if .passwordSecretName }}
- name: UC_HIBERNATE_CONNECTION_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .passwordSecretName | quote }}
      key: {{ required "Password secret key is required" .passwordSecretKey | quote }}
{{- end }}
{{- end }}
{{- range $index, $config := .Values.storage.credentials.s3 }}
- name: UC_S3_ACCESSKEY_{{ $index }}
  valueFrom:
    secretKeyRef:
      name: {{ $config.credentialsSecretName | quote }}
      key: accessKey
- name: UC_S3_SECRETKEY_{{ $index }}
  valueFrom:
    secretKeyRef:
      name: {{ $config.credentialsSecretName | quote }}
      key: secretKey
{{- end }}
{{- range $index, $config := .Values.storage.credentials.adls }}
- name: UC_ADLS_TENANTID_{{ $index }}
  valueFrom:
    secretKeyRef:
      name: {{ $config.credentialsSecretName | quote }}
      key: tenantId
- name: UC_ADLS_CLIENTID_{{ $index }}
  valueFrom:
    secretKeyRef:
      name: {{ $config.credentialsSecretName | quote }}
      key: clientId
- name: UC_ADLS_CLIENTSECRET_{{ $index }}
  valueFrom:
    secretKeyRef:
      name: {{ $config.credentialsSecretName | quote }}
      key: clientSecret
{{- end }}
{{- end }}
