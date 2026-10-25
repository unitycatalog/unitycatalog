# Deploy with Helm

Starting with 0.8.0, each Unity Catalog release publishes a Helm chart to Docker Hub with the same version as the
release. Chart version X.Y.Z runs the server image `unitycatalog/unitycatalog:vX.Y.Z` and the UI image
`unitycatalog/unitycatalog-ui:vX.Y.Z`.

## Prerequisites

- A Kubernetes 1.20 or later cluster
- Helm 3.8 or later, which can install charts from OCI registries

## Install the chart

```sh
helm install unitycatalog oci://registry-1.docker.io/unitycatalog/unitycatalog-chart \
  --version X.Y.Z --namespace unitycatalog --create-namespace
```

Replace `X.Y.Z` with the Unity Catalog release to install.

With the default values, the chart runs one server, which keeps its H2 database on a persistent volume. Run
`helm test unitycatalog --namespace unitycatalog` to check that the server responds.

## Open the API and the UI

```sh
kubectl port-forward --namespace unitycatalog svc/unitycatalog-unitycatalog-chart-server 8080:8080
```

The API is then at `http://localhost:8080/api/2.1/unity-catalog`.

The UI is off by default, because it runs a development server. To run it, add `--set ui.enabled=true` to the install
command. `helm test` then also checks the UI; with authentication on, it checks the UI only. By default, the UI forwards
API requests to the server, and in this mode you open it through `kubectl port-forward`:

```sh
kubectl port-forward --namespace unitycatalog svc/unitycatalog-unitycatalog-chart-ui 3000:3000
```

The UI is then at `http://localhost:3000`.

To serve the API and the UI on a hostname, enable `ingress` or `httpRoute`. They send `/api` requests to the server and everything
else to the UI. See
[values.httproute.yaml](https://github.com/unitycatalog/unitycatalog/blob/main/helm/values.httproute.yaml) for an
example.

## Use PostgreSQL

The H2 database supports a single server. To run more than one server replica, use PostgreSQL or
[MySQL](#use-mysql). The server then starts with an empty database; the chart leaves H2 data on its volume. Store the
password in a Secret:

```sh
kubectl create secret generic db-password --namespace unitycatalog --from-literal=password='<password>'
```

Then point the chart at the database, for example in a `values.yaml` file:

```yaml
server:
  deployment:
    replicaCount: 2
  db:
    type: postgresql
    postgresqlConfig:
      host: my-postgres.example.com
      port: 5432
      dbName: ucdb
      username: uc
      passwordSecretName: db-password
      passwordSecretKey: password
```

With authentication on (`auth.enabled: true`), PostgreSQL also needs `server.authorization.policy-refresh: "true"` under
`server.config.extraProperties`. Replicas share the database, so a grant or revoke made through one server must reach
the others. See
[Multiple server instances](auth.md#multiple-server-instances).

```sh
helm upgrade --install unitycatalog oci://registry-1.docker.io/unitycatalog/unitycatalog-chart \
  --version X.Y.Z --namespace unitycatalog --values values.yaml
```

## Use MySQL

The server image does not include the MySQL JDBC driver. Put the driver jar
([MySQL Connector/J](https://dev.mysql.com/downloads/connector/j/)) on a volume and point
`server.db.mysqlConfig.driver.volume` at it. The chart mounts the volume into the server and loads every jar on it.
The servers and the [pre-upgrade check](#upgrade-and-uninstall) mount it at the same time, so use a volume that
several pods can mount, for example a PersistentVolumeClaim with access mode `ReadOnlyMany`. The jar is too large for
a ConfigMap.

Create the database with a binary collation, as described in
[Example MySQL Connection](deployment.md#example-mysql-connection), and store the password in a Secret as for
PostgreSQL. Then set:

```yaml
server:
  db:
    type: mysql
    mysqlConfig:
      host: my-mysql.example.com
      port: 3306
      dbName: ucdb
      username: uc
      passwordSecretName: db-password
      passwordSecretKey: password
      driver:
        volume:
          persistentVolumeClaim:
            claimName: mysql-driver
```

`server.db.mysqlConfig.extraParams` adds parameters to the JDBC URL, for example `sslMode`. With authentication on,
set `server.authorization.policy-refresh: "true"` as for PostgreSQL.

## Upgrade and uninstall

To upgrade, read the release notes, back up the database, then run `helm upgrade` with the new `--version`. A new
release can upgrade the database schema when its server starts (see [Upgrades](upgrades.md)), so the chart upgrades
in this order:

1. With PostgreSQL or MySQL, a `pre-upgrade` hook Job runs the new release's `bin/start-uc-server --check-schema`,
   which changes nothing. If the new release would refuse the upgrade, the Job fails and so does `helm upgrade`,
   before it changes any resource; the old servers keep serving.
2. The chart stops all old servers, then starts the new ones (update strategy `Recreate`), because old and new
   servers must not run at once while the schema changes. The API is down until a new server is ready.
3. The first new server applies the pending schema steps, then serves. Other replicas wait for it.

If the check fails, read the Job's log. It says why, for example which rows block the upgrade:

```sh
kubectl logs --namespace unitycatalog job/unitycatalog-unitycatalog-chart-server-upgrade-check
```

Fix the cause as the release notes describe, then run `helm upgrade` again; it replaces the failed Job. To skip the
check, set `server.upgradeCheck.enabled: false`. With the default H2 database there is no check Job: the server checks
and upgrades the file on its volume when it starts.

Kubernetes restarts a server that has not opened its port within the startup probe's `failureThreshold ×
periodSeconds`, 30 minutes by default. For a longer upgrade, for example a MySQL upgrade that rebuilds large tables,
set the whole probe with a larger `failureThreshold`:

```yaml
server:
  deployment:
    startupProbe:
      tcpSocket:
        port: api
      timeoutSeconds: 10
      periodSeconds: 10
      failureThreshold: 720 # 2 hours
```

To have the server copy the database before it upgrades the schema, and restore the copy if the upgrade fails, set
`server.migration.backup.enabled: true` (see [Backup before the upgrade](upgrades.md#backup-before-the-upgrade)). The
database user must be able to create and drop databases (PostgreSQL: `CREATEDB` and owner of the Unity Catalog
database), and the database server, or with H2 the database volume, needs room for a full copy. On SIGTERM during the
upgrade, the server restores the copy before it exits, so `server.deployment.terminationGracePeriodSeconds` (120
seconds by default) must cover 60 seconds plus the restore.

The chart passes its revision to the server (`UC_CHART_REVISION`), so every `helm upgrade` restarts the servers. When
an upgrade fails and the server restores the copy, it marks the database with that revision, and later starts in the
same revision refuse instead of upgrading and restoring again (see
[The restored mark with Helm](upgrades.md#the-restored-mark-with-helm)). The previous release starts on the restored
database, so `helm rollback`, or `helm upgrade --atomic`, which rolls back on its own, brings the old servers back.
With `--atomic`, give `--timeout` more time than the upgrade takes; otherwise the rollback stops the upgrade. Fix the
cause, then run `helm upgrade` again: its new revision clears the mark.

`helm rollback` does not undo a successful schema upgrade. To go back, restore the backup first, as described in
[Going back to the baseline release](upgrades.md#going-back-to-the-baseline-release).

If you installed the chart from a source checkout before 0.8.0, set `nameOverride: unitycatalog` in your values.
The chart is now named `unitycatalog-chart`, which changes the resource names. The override keeps the old names, so
the release keeps its database volume and signing key. The UI is now off by default, so also set `ui.enabled: true`
to keep it.

`helm uninstall unitycatalog --namespace unitycatalog` removes the release. It keeps the H2 database
PersistentVolumeClaim and the token signing key Secret, and a later install with the same release name and
namespace reuses them. Delete both to remove the data.

## Configuration

The [chart README](https://github.com/unitycatalog/unitycatalog/blob/main/helm/README.md) lists every value,
including authentication (`auth.*`), cloud storage credentials, and resources. See [Auth](auth.md) for how
authentication works.
