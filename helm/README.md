# unitycatalog

![Version: 0.0.1-pre.1](https://img.shields.io/badge/Version-0.0.1--pre.1-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: main](https://img.shields.io/badge/AppVersion-main-informational?style=flat-square)

This chart deploys [Unity Catalog](https://github.com/unitycatalog/unitycatalog) on a Kubernetes cluster
using the [Helm](https://github.com/helm/helm) package manager.

## Prerequisites
- Kubernetes 1.20+ cluster
- Helm 3.1+

## Features
- Deploys Unity Catalog server and UI
- Supports OAuth authentication
- Supports file H2DB and PostgreSQL as a database
- Customizable configuration for server and UI using Helm values

## Installing the Chart
To install the chart with the release name `unitycatalog`:

```sh
helm install unitycatalog .
```

## Uninstalling the Chart
To uninstall the `unitycatalog` chart:

```sh
helm uninstall unitycatalog
```

The command removes all the Kubernetes components associated with the chart and deletes the release.
If persistent volumes are used, they are not deleted by default. To delete them, you need to manually delete the PVCs.

## Configuration
The following table lists the configurable parameters of the Unity Catalog chart and their default values.

> **Note**
>
> Auth integration in UI works only with Google OAuth provider and
> it does not work with UC versions 0.2.1 and below.

## Values

<table>
	<thead>
		<th>Key</th>
		<th>Type</th>
		<th>Default</th>
		<th>Description</th>
	</thead>
	<tbody>
		<tr>
			<td>auth.authorizationUrl</td>
			<td>string</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>OAuth authorization URL

Example: `https://accounts.google.com/o/oauth2/auth`
</td>
		</tr>
		<tr>
			<td>auth.clientSecretName</td>
			<td>string</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>OAuth client secret name

Client secret must contain the following keys:
- `clientId`: OAuth client ID
- `clientSecret`: OAuth client secret
</td>
		</tr>
		<tr>
			<td>auth.cookieTimeout</td>
			<td>string</td>
			<td><pre lang="json">
"P5D"
</pre>
</td>
			<td>OAuth cookie timeout

Example: `P5D` (5 days)
</td>
		</tr>
		<tr>
			<td>auth.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Enable OAuth authentication</td>
		</tr>
		<tr>
			<td>auth.provider</td>
			<td>string</td>
			<td><pre lang="json">
"google"
</pre>
</td>
			<td>OAuth provider

Supported values: `google`, `other`

other: Use this option if you want to use a custom OAuth provider. UI does not have any built-in support for this option.
</td>
		</tr>
		<tr>
			<td>auth.redirectPort</td>
			<td>int</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>OAuth redirect port</td>
		</tr>
		<tr>
			<td>auth.tokenUrl</td>
			<td>string</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>OAuth token URL

Example: `https://oauth2.googleapis.com/token`
</td>
		</tr>
		<tr>
			<td>auth.users</td>
			<td>array</td>
			<td><pre lang="yaml">
not set
</pre>
</td>
			<td>List of users to be created in the system

TODO: It is not possible to create proper admin account. This is a workaround.

Example:
```yaml
users:
- name: admin
  email: test@example.com
  canCreateCatalogs: true    # Optional (default: false)
```
</td>
		</tr>
		<tr>
			<td>fullnameOverride</td>
			<td>string</td>
			<td><pre lang="">
derived from the chart parameters and metadata
</pre>
</td>
			<td>Override the full name of the chart</td>
		</tr>
		<tr>
			<td>httpRoute.annotations</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>HTTPRoute annotations</td>
		</tr>
		<tr>
			<td>httpRoute.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Enable httpRoute for the server

[Kubernetes docs](https://gateway-api.sigs.k8s.io/api-types/httproute/)
</td>
		</tr>
		<tr>
			<td>httpRoute.host</td>
			<td>array</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>HTTPRoute host configuration (Required for httpRoute)</td>
		</tr>
		<tr>
			<td>httpRoute.parentRefs</td>
			<td>string</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>HTTPRoute parent references

Example:
```yaml
parentRefs:
  - name: my-gateway
    namespace: my-namespace
    kind: Gateway
    group: gateway.networking.k8s.io
```
</td>
		</tr>
		<tr>
			<td>ingress.annotations</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Ingress annotations</td>
		</tr>
		<tr>
			<td>ingress.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Enable ingress for the server

[Kubernetes docs](https://kubernetes.io/docs/concepts/services-networking/ingress/)
</td>
		</tr>
		<tr>
			<td>ingress.host</td>
			<td>string</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Ingress host configuration (Required for TLS)</td>
		</tr>
		<tr>
			<td>ingress.ingressClassName</td>
			<td>string</td>
			<td><pre lang="">
default ingress class
</pre>
</td>
			<td>Ingress class name</td>
		</tr>
		<tr>
			<td>ingress.tlsSecretName</td>
			<td>string</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Ingress TLS secret name

If set, TLS will be enabled for the ingress
</td>
		</tr>
		<tr>
			<td>nameOverride</td>
			<td>string</td>
			<td><pre lang="">
derived from the chart parameters and metadata
</pre>
</td>
			<td>Override the name of the chart</td>
		</tr>
		<tr>
			<td>server.config.extraProperties</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Extra config properties that will be added to server.properties

Example:
```yaml
extraProperties:
  my-config.property1: my-value1
  my-config.property2: my-value2
```
</td>
		</tr>
		<tr>
			<td>server.config.override</td>
			<td>string</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Server configuration template override

Example:
```
server.env=prod
server.port=8080
...other config properties...
```
</td>
		</tr>
		<tr>
			<td>server.createUsersJob.backoffLimit</td>
			<td>int</td>
			<td><pre lang="">
5
</pre>
</td>
			<td>Backoff limit for the create users job

[Kubernetes docs](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/)
</td>
		</tr>
		<tr>
			<td>server.createUsersJob.extraAnnotations</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Additional annotations for the create users job

[Kubernetes docs](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/)
</td>
		</tr>
		<tr>
			<td>server.createUsersJob.image.pullPolicy</td>
			<td>string</td>
			<td><pre lang="json">
"IfNotPresent"
</pre>
</td>
			<td>Pull policy for the create users job image</td>
		</tr>
		<tr>
			<td>server.createUsersJob.image.repository</td>
			<td>string</td>
			<td><pre lang="json">
"alpine"
</pre>
</td>
			<td>Repository for the create users job image</td>
		</tr>
		<tr>
			<td>server.createUsersJob.image.tag</td>
			<td>string</td>
			<td><pre lang="json">
"latest"
</pre>
</td>
			<td>Tag for the create users job image</td>
		</tr>
		<tr>
			<td>server.createUsersJob.ttlSecondsAfterFinished</td>
			<td>int</td>
			<td><pre lang="">
300
</pre>
</td>
			<td>TTL for the create users job (in seconds)

[Kubernetes docs](https://kubernetes.io/docs/concepts/workloads/controllers/ttlafterfinished/)
</td>
		</tr>
		<tr>
			<td>server.db.fileConfig.persistence.accessModes</td>
			<td>array</td>
			<td><pre lang="json">
[
  "ReadWriteOnce"
]
</pre>
</td>
			<td>Access modes for the file database volume</td>
		</tr>
		<tr>
			<td>server.db.fileConfig.persistence.createPVC</td>
			<td>bool</td>
			<td><pre lang="json">
true
</pre>
</td>
			<td>Create a new file database volume if it does not exist</td>
		</tr>
		<tr>
			<td>server.db.fileConfig.persistence.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
true
</pre>
</td>
			<td>Enable persistence for the file database volume

If set to false, all data will be lost when the pod is deleted
</td>
		</tr>
		<tr>
			<td>server.db.fileConfig.persistence.pvcName</td>
			<td>string</td>
			<td><pre lang="">
derived from the chart parameters and metadata
</pre>
</td>
			<td>Name of the file database persistence volume claim</td>
		</tr>
		<tr>
			<td>server.db.fileConfig.persistence.size</td>
			<td>string</td>
			<td><pre lang="json">
"1Gi"
</pre>
</td>
			<td>Size of the file database volume

Example: `1Gi`
</td>
		</tr>
		<tr>
			<td>server.db.fileConfig.persistence.storageClassName</td>
			<td>string</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Storage class for the file database volume

If not set, the default storage class will be used
</td>
		</tr>
		<tr>
			<td>server.db.overrideConfig</td>
			<td>string</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Override Hibernate configuration for the server</td>
		</tr>
		<tr>
			<td>server.db.postgresqlConfig.dbName</td>
			<td>string</td>
			<td><pre lang="">
ucdb
</pre>
</td>
			<td>PostgreSQL database name</td>
		</tr>
		<tr>
			<td>server.db.postgresqlConfig.extraParams</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Postgres params

Example:
```yaml
params:
  sslfactory: my.custom.SSLFactory
```
</td>
		</tr>
		<tr>
			<td>server.db.postgresqlConfig.host</td>
			<td>string</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>PostgreSQL database host (Required)

Example: `mydb.123456789012.us-east-1.rds.amazonaws.com`
</td>
		</tr>
		<tr>
			<td>server.db.postgresqlConfig.passwordSecretKey</td>
			<td>string</td>
			<td><pre lang="">
password
</pre>
</td>
			<td>PostgreSQL user password secret key</td>
		</tr>
		<tr>
			<td>server.db.postgresqlConfig.passwordSecretName</td>
			<td>string</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>PostgreSQL user password secret name

If not set, connection will be made without a password
</td>
		</tr>
		<tr>
			<td>server.db.postgresqlConfig.port</td>
			<td>int</td>
			<td><pre lang="">
5432
</pre>
</td>
			<td>PostgreSQL database port</td>
		</tr>
		<tr>
			<td>server.db.postgresqlConfig.ssl.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td></td>
		</tr>
		<tr>
			<td>server.db.postgresqlConfig.ssl.mode</td>
			<td>string</td>
			<td><pre lang="">
verify-full
</pre>
</td>
			<td>PostgreSQL SSL mode

[Documentation](https://jdbc.postgresql.org/documentation/ssl/)
</td>
		</tr>
		<tr>
			<td>server.db.postgresqlConfig.ssl.rootCert</td>
			<td>string</td>
			<td><pre lang="">
public root CAs
</pre>
</td>
			<td>PostgreSQL SSL root certificate

Example:
```yaml
rootCert: |
  -----BEGIN CERTIFICATE-----
  ...certificate data...
  -----END CERTIFICATE-----
```
</td>
		</tr>
		<tr>
			<td>server.db.postgresqlConfig.ssl.rootCertConfigMapKey</td>
			<td>string</td>
			<td><pre lang="">
ca.crt
</pre>
</td>
			<td>PostgreSQL SSL root certificate config map key</td>
		</tr>
		<tr>
			<td>server.db.postgresqlConfig.ssl.rootCertConfigMapName</td>
			<td>string</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>PostgreSQL SSL root certificate config map name</td>
		</tr>
		<tr>
			<td>server.db.postgresqlConfig.username</td>
			<td>string</td>
			<td><pre lang="">
postgres
</pre>
</td>
			<td>PostgreSQL username</td>
		</tr>
		<tr>
			<td>server.db.type</td>
			<td>string</td>
			<td><pre lang="json">
"file"
</pre>
</td>
			<td>Database type

Supported values: `file`, `postgresql`
</td>
		</tr>
		<tr>
			<td>server.deployment.affinity</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Node affinity for the deployment

[Kubernetes docs](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/)
</td>
		</tr>
		<tr>
			<td>server.deployment.enableLivenessProbe</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Enable liveness probe

If set to true, the liveness probe will be enabled.

[Kubernetes docs](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
</td>
		</tr>
		<tr>
			<td>server.deployment.enableReadinessProbe</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Enable readiness probe

If set to true, the readiness probe will be enabled.

[Kubernetes docs](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
</td>
		</tr>
		<tr>
			<td>server.deployment.enableStartupProbe</td>
			<td>bool</td>
			<td><pre lang="json">
true
</pre>
</td>
			<td>Enable startup probe

If set to true, the startup probe will be enabled.

[Kubernetes docs](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
</td>
		</tr>
		<tr>
			<td>server.deployment.extraContainers</td>
			<td>array</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>List of additional containers to be added to the server pod

[Kubernetes docs](https://kubernetes.io/docs/concepts/workloads/pods/#pod)
</td>
		</tr>
		<tr>
			<td>server.deployment.extraInitContainers</td>
			<td>array</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>List of additional init containers to be added to the server pod

[Kubernetes docs](https://kubernetes.io/docs/concepts/workloads/pods/#pod)
</td>
		</tr>
		<tr>
			<td>server.deployment.extraPodAnnotations</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Additional annotations for the pod

[Kubernetes docs](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/)
</td>
		</tr>
		<tr>
			<td>server.deployment.extraPodLabels</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Additional labels for the pod

[Kubernetes docs](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/)
</td>
		</tr>
		<tr>
			<td>server.deployment.extraVolumeMounts</td>
			<td>array</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>List of additional volume mounts for the pod

[Kubernetes docs](https://kubernetes.io/docs/concepts/storage/volumes/)
</td>
		</tr>
		<tr>
			<td>server.deployment.extraVolumes</td>
			<td>array</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>List of additional volumes to be mounted in the pod

[Kubernetes docs](https://kubernetes.io/docs/concepts/storage/volumes/)
</td>
		</tr>
		<tr>
			<td>server.deployment.image.pullPolicy</td>
			<td>string</td>
			<td><pre lang="json">
"IfNotPresent"
</pre>
</td>
			<td>Pull policy for the server image</td>
		</tr>
		<tr>
			<td>server.deployment.image.repository</td>
			<td>string</td>
			<td><pre lang="json">
"unitycatalog/unitycatalog"
</pre>
</td>
			<td>Repository for the server image</td>
		</tr>
		<tr>
			<td>server.deployment.image.tag</td>
			<td>string</td>
			<td><pre lang="">
{{ .Chart.AppVersion }}
</pre>
</td>
			<td>Tag for the server image</td>
		</tr>
		<tr>
			<td>server.deployment.imagePullSecrets</td>
			<td>array</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Image pull secrets for the server

[Kubernetes docs](https://kubernetes.io/docs/concepts/containers/images/#specifying-imagepullsecrets-on-a-pod)
</td>
		</tr>
		<tr>
			<td>server.deployment.initContainer.image.pullPolicy</td>
			<td>string</td>
			<td><pre lang="json">
"IfNotPresent"
</pre>
</td>
			<td>Pull policy for the render config container</td>
		</tr>
		<tr>
			<td>server.deployment.initContainer.image.repository</td>
			<td>string</td>
			<td><pre lang="json">
"alpine"
</pre>
</td>
			<td>Repository for the render config container</td>
		</tr>
		<tr>
			<td>server.deployment.initContainer.image.tag</td>
			<td>string</td>
			<td><pre lang="json">
"latest"
</pre>
</td>
			<td>Tag for the render config container</td>
		</tr>
		<tr>
			<td>server.deployment.initContainer.securityContext</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Security context for the init container

[Kubernetes docs](https://kubernetes.io/docs/concepts/policy/security-context/)
</td>
		</tr>
		<tr>
			<td>server.deployment.livenessProbe</td>
			<td>object</td>
			<td><pre lang="">
tcpSocket:<br>  port: api<br>timeoutSeconds: 10<br>periodSeconds: 10<br>failureThreshold: 30 # 5 minutes
</pre>
</td>
			<td>Liveness probe

[Kubernetes docs](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
</td>
		</tr>
		<tr>
			<td>server.deployment.nodeSelector</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Node selector for the deployment

[Kubernetes docs](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/)
</td>
		</tr>
		<tr>
			<td>server.deployment.podSecurityContext</td>
			<td>object</td>
			<td><pre lang="json">
{
  "fsGroup": 101
}
</pre>
</td>
			<td>Security context for the pod

By default, the pod security context has `fsGroup` set to `101` to allow the container
to write to the mounted volumes.

[Kubernetes docs](https://kubernetes.io/docs/concepts/policy/security-context/)
</td>
		</tr>
		<tr>
			<td>server.deployment.port</td>
			<td>int</td>
			<td><pre lang="json">
8080
</pre>
</td>
			<td>Port for the server deployment</td>
		</tr>
		<tr>
			<td>server.deployment.readinessProbe</td>
			<td>object</td>
			<td><pre lang="">
tcpSocket:<br>  port: api<br>timeoutSeconds: 10<br>periodSeconds: 10<br>failureThreshold: 30 # 5 minutes
</pre>
</td>
			<td>Readiness probe

[Kubernetes docs](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
</td>
		</tr>
		<tr>
			<td>server.deployment.replicaCount</td>
			<td>int</td>
			<td><pre lang="json">
1
</pre>
</td>
			<td>Number of replicas

For replica count higher than 1 - use an external database
</td>
		</tr>
		<tr>
			<td>server.deployment.resources</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Resources for the deployment

[Kubernetes docs](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
</td>
		</tr>
		<tr>
			<td>server.deployment.securityContext</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Security context for the container

[Kubernetes docs](https://kubernetes.io/docs/concepts/policy/security-context/)
</td>
		</tr>
		<tr>
			<td>server.deployment.startupProbe</td>
			<td>object</td>
			<td><pre lang="">
tcpSocket:<br>  port: api<br>timeoutSeconds: 10<br>periodSeconds: 10<br>failureThreshold: 30 # 5 minutes
</pre>
</td>
			<td>Startup probe

[Kubernetes docs](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
</td>
		</tr>
		<tr>
			<td>server.deployment.tolerations</td>
			<td>array</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Tolerations for the deployment

[Kubernetes docs](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/)
</td>
		</tr>
		<tr>
			<td>server.deployment.updateStrategy</td>
			<td>object</td>
			<td><pre lang="">
# If replicaCount: 1 and db type "file"<br>type: Recreate<br># Else<br>type: RollingUpdate
</pre>
</td>
			<td>Update strategy for the deployment

[Kubernetes docs](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#updating-a-deployment)
</td>
		</tr>
		<tr>
			<td>server.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
true
</pre>
</td>
			<td>Enable the server deployment</td>
		</tr>
		<tr>
			<td>server.hibernateConfigOverride</td>
			<td>string</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Server hibernate configuration template override

Example:
```
hibernate.dialect=org.hibernate.dialect.MySQLDialect
hibernate.connection.driver_class=com.mysql.cj.jdbc.Driver
...other hibernate config properties...
```
</td>
		</tr>
		<tr>
			<td>server.jwtKeypairSecret.create</td>
			<td>bool</td>
			<td><pre lang="json">
true
</pre>
</td>
			<td>Create keypair secret

If set to true, the secret will be created with a random keypair
</td>
		</tr>
		<tr>
			<td>server.jwtKeypairSecret.name</td>
			<td>string</td>
			<td><pre lang="">
derived from the chart parameters and metadata
</pre>
</td>
			<td>Name of the keypair secret for JWT signing

The secret must contain the following keys:
- `private_key.der` or `private_key.pem`: Private key in DER or PEM format
- `key_id.txt`: Key ID

[Kubernetes docs](https://github.com/unitycatalog/unitycatalog/blob/main/server/src/main/java/io/unitycatalog/server/security/SecurityConfiguration.java)
</td>
		</tr>
		<tr>
			<td>server.logLevel</td>
			<td>string</td>
			<td><pre lang="">
info
</pre>
</td>
			<td>Log level for the server</td>
		</tr>
		<tr>
			<td>server.service.port</td>
			<td>int</td>
			<td><pre lang="json">
8080
</pre>
</td>
			<td>Service port for the server</td>
		</tr>
		<tr>
			<td>server.service.type</td>
			<td>string</td>
			<td><pre lang="json">
"ClusterIP"
</pre>
</td>
			<td>Service type for the server</td>
		</tr>
		<tr>
			<td>serviceAccount.annotations</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Annotations for the service account</td>
		</tr>
		<tr>
			<td>serviceAccount.automount</td>
			<td>bool</td>
			<td><pre lang="json">
true
</pre>
</td>
			<td>Automount the service account token</td>
		</tr>
		<tr>
			<td>serviceAccount.create</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Create a service account</td>
		</tr>
		<tr>
			<td>serviceAccount.name</td>
			<td>string</td>
			<td><pre lang="">
derived from the chart parameters and metadata
</pre>
</td>
			<td>Name of the service account</td>
		</tr>
		<tr>
			<td>storage.credentials.adls</td>
			<td>array</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>ADLS credentials for accessing the storage

Example:
```yaml
adls:
- storageAccountName: my-storage-account
  credentialsSecretName: my-storage-secret
```

Credential secret must contain the following keys:
- `tenantId`: Azure tenant ID
- `clientId`: Azure client ID
- `clientSecret`: Azure client secret
</td>
		</tr>
		<tr>
			<td>storage.credentials.gcs</td>
			<td>array</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>GCS credentials for accessing the storage

Example:
```yaml
gcs:
- bucketPath: gs://my-bucket-name
  credentialsSecretName: my-bucket-secret
```

Credential secret must contain the following keys:
- `jsonKey`: GCP service account JSON key
</td>
		</tr>
		<tr>
			<td>storage.credentials.s3</td>
			<td>array</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>S3 credentials for accessing the storage

Example:
```yaml
s3:
- bucketPath: s3://my-bucket-path
  region: us-east-1
  awsRoleArn: arn:aws:iam::123456789012:role/my-role
  credentialsSecretName: my-bucket-secret
```

Credential secret must contain the following keys:
- `accessKey`: AWS access key
- `secretKey`: AWS secret key
</td>
		</tr>
		<tr>
			<td>storage.modelStorageRoot</td>
			<td>string</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Root path for the model storage

Example: `s3://bucket/path`
</td>
		</tr>
		<tr>
			<td>ui.deployment.affinity</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Node affinity for the UI deployment

[Kubernetes docs](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.enableLivenessProbe</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Enable liveness probe

If set to true, the liveness probe will be enabled.

[Kubernetes docs](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.enableReadinessProbe</td>
			<td>bool</td>
			<td><pre lang="json">
false
</pre>
</td>
			<td>Enable readiness probe

If set to true, the readiness probe will be enabled.

[Kubernetes docs](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.enableStartupProbe</td>
			<td>bool</td>
			<td><pre lang="json">
true
</pre>
</td>
			<td>Enable startup probe

If set to true, the startup probe will be enabled.

[Kubernetes docs](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.extraContainers</td>
			<td>array</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>List of additional containers to be added to the UI pods

[Kubernetes docs](https://kubernetes.io/docs/concepts/workloads/pods/#pod)
</td>
		</tr>
		<tr>
			<td>ui.deployment.extraInitContainers</td>
			<td>array</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>List of additional init containers to be added to the UI pods

[Kubernetes docs](https://kubernetes.io/docs/concepts/workloads/pods/#pod)
</td>
		</tr>
		<tr>
			<td>ui.deployment.extraPodAnnotations</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Additional annotations for the UI pods

[Kubernetes docs](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.extraPodLabels</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Additional labels for the UI pods

[Kubernetes docs](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.extraVolumeMounts</td>
			<td>array</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>List of additional volume mounts for the pod

[Kubernetes docs](https://kubernetes.io/docs/concepts/storage/volumes/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.extraVolumes</td>
			<td>array</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>List of additional volumes to be mounted in the pod

[Kubernetes docs](https://kubernetes.io/docs/concepts/storage/volumes/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.image.pullPolicy</td>
			<td>string</td>
			<td><pre lang="json">
"IfNotPresent"
</pre>
</td>
			<td>Pull policy for the UI image</td>
		</tr>
		<tr>
			<td>ui.deployment.image.repository</td>
			<td>string</td>
			<td><pre lang="json">
"unitycatalog/unitycatalog-ui"
</pre>
</td>
			<td>Repository for the UI image</td>
		</tr>
		<tr>
			<td>ui.deployment.image.tag</td>
			<td>string</td>
			<td><pre lang="">
{{ .Chart.AppVersion }}
</pre>
</td>
			<td>Tag for the UI image</td>
		</tr>
		<tr>
			<td>ui.deployment.imagePullSecrets</td>
			<td>array</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Image pull secrets for the UI

[Kubernetes docs](https://kubernetes.io/docs/concepts/containers/images/#specifying-imagepullsecrets-on-a-pod)
</td>
		</tr>
		<tr>
			<td>ui.deployment.initContainer.image.pullPolicy</td>
			<td>string</td>
			<td><pre lang="json">
"IfNotPresent"
</pre>
</td>
			<td>Pull policy for the UI init container</td>
		</tr>
		<tr>
			<td>ui.deployment.initContainer.image.repository</td>
			<td>string</td>
			<td><pre lang="json">
"busybox"
</pre>
</td>
			<td>Repository for the UI init container</td>
		</tr>
		<tr>
			<td>ui.deployment.initContainer.image.tag</td>
			<td>string</td>
			<td><pre lang="json">
"latest"
</pre>
</td>
			<td>Tag for the UI init container</td>
		</tr>
		<tr>
			<td>ui.deployment.initContainer.securityContext</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Security context for the init container

[Kubernetes docs](https://kubernetes.io/docs/concepts/policy/security-context/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.livenessProbe</td>
			<td>object</td>
			<td><pre lang="">
tcpSocket:<br>  port: ui<br>timeoutSeconds: 10<br>periodSeconds: 10<br>failureThreshold: 30 # 5 minutes
</pre>
</td>
			<td>Liveness probe

[Kubernetes docs](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.nodeSelector</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Node selector for the UI deployment

[Kubernetes docs](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.podSecurityContext</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Security context for the UI pod

[Kubernetes docs](https://kubernetes.io/docs/concepts/policy/security-context/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.port</td>
			<td>int</td>
			<td><pre lang="json">
3000
</pre>
</td>
			<td>Port for the UI deployment</td>
		</tr>
		<tr>
			<td>ui.deployment.readinessProbe</td>
			<td>object</td>
			<td><pre lang="">
tcpSocket:<br>  port: ui<br>timeoutSeconds: 10<br>periodSeconds: 10<br>failureThreshold: 30 # 5 minutes
</pre>
</td>
			<td>Readiness probe

[Kubernetes docs](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.replicaCount</td>
			<td>int</td>
			<td><pre lang="json">
1
</pre>
</td>
			<td>Replica count for the UI deployment</td>
		</tr>
		<tr>
			<td>ui.deployment.resources</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Resources for the UI deployment

[Kubernetes docs](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.securityContext</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Security context for the UI container

[Kubernetes docs](https://kubernetes.io/docs/concepts/policy/security-context/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.startupProbe</td>
			<td>object</td>
			<td><pre lang="">
tcpSocket:<br>  port: ui<br>timeoutSeconds: 10<br>periodSeconds: 10<br>failureThreshold: 30 # 5 minutes
</pre>
</td>
			<td>Startup probe

[Kubernetes docs](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.tolerations</td>
			<td>array</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Tolerations for the UI deployment

[Kubernetes docs](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/)
</td>
		</tr>
		<tr>
			<td>ui.deployment.updateStrategy</td>
			<td>object</td>
			<td><pre lang="">
not set
</pre>
</td>
			<td>Update strategy for the deployment

[Kubernetes docs](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#updating-a-deployment)
</td>
		</tr>
		<tr>
			<td>ui.enabled</td>
			<td>bool</td>
			<td><pre lang="json">
true
</pre>
</td>
			<td>Enable the UI</td>
		</tr>
		<tr>
			<td>ui.proxyApiRequests</td>
			<td>bool</td>
			<td><pre lang="json">
null
</pre>
</td>
			<td>Enable proxy API requests

If set to true, the UI will proxy API requests to the server.
It will only accept requests from the server with the host header `localhost`.
For production deployment with a load balancer that routes traffic to the UI and server, set this to false.
</td>
		</tr>
		<tr>
			<td>ui.serverUrl</td>
			<td>tpl/string</td>
			<td><pre lang="">
http://{{ include "unitycatalog.server.fullname" . }}:{{ .Values.server.service.port }}
</pre>
</td>
			<td>Server URL for the UI to connect to

The server URL is used by the UI to connect to the server.
The default value is set to the server service URL.
</td>
		</tr>
		<tr>
			<td>ui.service.port</td>
			<td>int</td>
			<td><pre lang="json">
3000
</pre>
</td>
			<td>Service port for the UI</td>
		</tr>
		<tr>
			<td>ui.service.type</td>
			<td>string</td>
			<td><pre lang="json">
"ClusterIP"
</pre>
</td>
			<td>Service type for the UI</td>
		</tr>
	</tbody>
</table>

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.14.2](https://github.com/norwoodj/helm-docs/releases/v1.14.2)
