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
server.client-secret=${OAUTH_CLIENT_SECRET}
server.redirect-port={{ .Values.auth.redirectPort }}
server.cookie-timeout={{ .Values.auth.cookieTimeout }}
{{- end }}

storage-root.models={{ .Values.storage.modelStorageRoot }}

{{- range $index, $config := .Values.storage.credentials.s3 }}
s3.bucketPath.{{ $index }}={{ $config.bucketPath }}
s3.region.{{ $index }}={{ $config.region }}
s3.awsRoleArn.{{ $index }}={{ $config.awsRoleArn }}
s3.accessKey.{{ $index }}=${S3_ACCESS_KEY_{{ $index }}}
s3.secretKey.{{ $index }}=${S3_SECRET_KEY_{{ $index }}}
{{- end }}

{{- range $index, $config := .Values.storage.credentials.adls }}
adls.storageAccountName.{{ $index }}={{ $config.storageAccountName }}
adls.tenantId.{{ $index }}=${ADLS_TENANT_ID_{{ $index }}}
adls.clientId.{{ $index }}=${ADLS_CLIENT_ID_{{ $index }}}
adls.clientSecret.{{ $index }}=${ADLS_CLIENT_SECRET_{{ $index }}}
{{- end }}

{{- range $index, $config := .Values.storage.credentials.gcs }}
gcs.bucketPath.{{ $index }}={{ $config.bucketPath }}
{{- if $config.credentialsSecretName }}
gcs.jsonKeyFilePath.{{ $index }}=/etc/conf/gcs-credentials-{{ $index }}.json
{{- end }}
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
hibernate.connection.url=jdbc:h2:file:./etc/db/h2db;DB_CLOSE_DELAY=-1
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
{{- if .Values.server.db.postgresqlConfig.passwordSecretName }}
hibernate.connection.password=${DB_PASSWORD}
{{- end }}
{{- else }}
{{ fail "Unsupported database type. Supported types are: file, postgresql." }}
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
appenders=console

appender.console.type=Console
appender.console.name=Console
appender.console.layout.type=PatternLayout
appender.console.layout.pattern=%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n

rootLogger.level={{ .Values.server.logLevel }}
rootLogger.appenderRefs=console
rootLogger.appenderRef.console.ref=Console
{{- end }}
