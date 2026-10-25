{{/*
The server's init container: renders the configuration templates and prepares the token signing keys. The
pre-upgrade check Job runs it too, so both read the database with the same settings.
*/}}
{{- define "unitycatalog.server.initContainer" -}}
- name: init
  {{- with .Values.server.deployment.initContainer.securityContext }}
  securityContext: {{- toYaml . | nindent 4 }}
  {{- end }}
  image: "{{ .Values.server.deployment.initContainer.image.repository }}:{{ .Values.server.deployment.initContainer.image.tag }}"
  imagePullPolicy: {{ .Values.server.deployment.initContainer.image.pullPolicy | quote }}
  command:
    - /bin/sh
    - -c
    - |-
      apk add --no-cache envsubst openssl curl
      envsubst < /home/unitycatalog/etc/confTemplates/server.properties.template > /home/unitycatalog/etc/conf/server.properties
      envsubst < /home/unitycatalog/etc/confTemplates/server.log4j2.properties.template > /home/unitycatalog/etc/conf/server.log4j2.properties
      envsubst < /home/unitycatalog/etc/confTemplates/hibernate.properties.template > /home/unitycatalog/etc/conf/hibernate.properties

      echo "Generating DER format keys (private_key.der and public_key.der) if they do not exist..."
      if [ -f /home/unitycatalog/etc/conf/private_key.der ] && [ -f /home/unitycatalog/etc/conf/public_key.der ]; then
        echo "DER format keys already exist, skipping generation."
      elif [ -f /home/unitycatalog/etc/confJwtKey/private_key.der ]; then
        echo "Using /home/unitycatalog/etc/confJwtKey/private_key.der..."
        cp /home/unitycatalog/etc/confJwtKey/private_key.der /home/unitycatalog/etc/conf/private_key.der
        openssl rsa -pubout -inform DER -outform DER \
            -in /home/unitycatalog/etc/confJwtKey/private_key.der \
            -out /home/unitycatalog/etc/conf/public_key.der
      elif [ -f /home/unitycatalog/etc/confJwtKey/private_key.pem ]; then
        echo "Using /home/unitycatalog/etc/confJwtKey/private_key.pem..."
        openssl pkcs8 -topk8 -inform PEM -outform DER -nocrypt \
            -in /home/unitycatalog/etc/confJwtKey/private_key.pem \
            -out /home/unitycatalog/etc/conf/private_key.der
        openssl rsa -pubout -inform PEM -outform DER \
            -in /home/unitycatalog/etc/confJwtKey/private_key.pem \
            -out /home/unitycatalog/etc/conf/public_key.der
      else
        echo "ERROR: No private_key.der or private_key.pem found in /home/unitycatalog/etc/conf or /home/unitycatalog/etc/confJwtKey"
        exit 1
      fi

      echo "Looking up key_id.txt..."
      if [ -f /home/unitycatalog/etc/conf/key_id.txt ]; then
        echo "Key ID file already exists, skipping..."
      elif [ -f /home/unitycatalog/etc/confJwtKey/key_id.txt ]; then
        echo "Using /home/unitycatalog/etc/confJwtKey/key_id.txt..."
        cp /home/unitycatalog/etc/confJwtKey/key_id.txt /home/unitycatalog/etc/conf/key_id.txt
      else
        echo "ERROR: No key_id.txt found in /home/unitycatalog/etc/confJwtKey"
        exit 1
      fi

      {{- if and (eq .Values.server.db.type "postgresql") .Values.server.db.postgresqlConfig.ssl.enabled }}
      echo "${DB_ROOT_CERT}" > /home/unitycatalog/etc/conf/dbrootca.crt
      chmod 0600 /home/unitycatalog/etc/conf/dbrootca.crt
      {{- end }}

      # Set permissions for the mounted volumes uid:gid 100:101 - unitycatalog:unitycatalog
      chown -R 100:101 /home/unitycatalog/etc/conf
  volumeMounts:
    - name: config-templates
      mountPath: /home/unitycatalog/etc/confTemplates
    - name: config-volume
      mountPath: /home/unitycatalog/etc/conf
    - name: jwt-key-volume
      mountPath: /home/unitycatalog/etc/confJwtKey
  env:
  {{- if and (eq .Values.server.db.type "postgresql") .Values.server.db.postgresqlConfig.passwordSecretName }}
    - name: DB_PASSWORD
      valueFrom:
        secretKeyRef:
          name: {{ .Values.server.db.postgresqlConfig.passwordSecretName | quote }}
          key: {{ required "Password secret key is required" .Values.server.db.postgresqlConfig.passwordSecretKey | quote }}
  {{- end }}
  {{- if and (eq .Values.server.db.type "mysql") .Values.server.db.mysqlConfig.passwordSecretName }}
    - name: DB_PASSWORD
      valueFrom:
        secretKeyRef:
          name: {{ .Values.server.db.mysqlConfig.passwordSecretName | quote }}
          key: {{ required "Password secret key is required" .Values.server.db.mysqlConfig.passwordSecretKey | quote }}
  {{- end }}
  {{- if and (eq .Values.server.db.type "postgresql") .Values.server.db.postgresqlConfig.ssl.enabled }}
  {{- if .Values.server.db.postgresqlConfig.ssl.rootCert }}
    - name: DB_ROOT_CERT
      value: |
        {{- .Values.server.db.postgresqlConfig.ssl.rootCert | nindent 8 }}
  {{- else if .Values.server.db.postgresqlConfig.ssl.rootCertConfigMapName }}
    - name: DB_ROOT_CERT
      valueFrom:
        configMapKeyRef:
          name: {{ .Values.server.db.postgresqlConfig.ssl.rootCertConfigMapName | quote }}
          key: {{ required "Root certificate config map key is required" .Values.server.db.postgresqlConfig.ssl.rootCertConfigMapKey | quote }}
  {{- else }}
    {{ fail "Either rootCert or rootCertConfigMapName must be set when ssl is enabled" }}
  {{- end }}
  {{- end }}
  {{- range $index, $config := .Values.storage.credentials.s3 }}
    - name: S3_ACCESS_KEY_{{ $index }}
      valueFrom:
        secretKeyRef:
          name: {{ $config.credentialsSecretName | quote }}
          key: accessKey
    - name: S3_SECRET_KEY_{{ $index }}
      valueFrom:
        secretKeyRef:
          name: {{ $config.credentialsSecretName | quote }}
          key: secretKey
  {{- end }}
  {{- range $index, $config := .Values.storage.credentials.adls }}
    - name: ADLS_TENANT_ID_{{ $index }}
      valueFrom:
        secretKeyRef:
          name: {{ $config.credentialsSecretName | quote }}
          key: tenantId
    - name: ADLS_CLIENT_ID_{{ $index }}
      valueFrom:
        secretKeyRef:
          name: {{ $config.credentialsSecretName | quote }}
          key: clientId
    - name: ADLS_CLIENT_SECRET_{{ $index }}
      valueFrom:
        secretKeyRef:
          name: {{ $config.credentialsSecretName | quote }}
          key: clientSecret
  {{- end }}
  {{- if .Values.auth.enabled }}
    - name: OAUTH_CLIENT_ID
      valueFrom:
        secretKeyRef:
          name: {{ .Values.auth.clientSecretName | quote }}
          key: clientId
    - name: OAUTH_CLIENT_SECRET
      valueFrom:
        secretKeyRef:
          name: {{ .Values.auth.clientSecretName | quote }}
          key: clientSecret
  {{- end }}
{{- end }}
