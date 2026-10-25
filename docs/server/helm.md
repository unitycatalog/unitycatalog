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

The H2 database supports a single server. To run more than one server replica, use PostgreSQL. The server then starts
with an empty database; the chart leaves H2 data on its volume. Store the password in a Secret:

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
`server.config.extraProperties`. Replicas share the database, and even a single replica runs next to its replacement
during an upgrade, so a grant or revoke made through one server must reach the others. See
[Multiple server instances](auth.md#multiple-server-instances).

```sh
helm upgrade --install unitycatalog oci://registry-1.docker.io/unitycatalog/unitycatalog-chart \
  --version X.Y.Z --namespace unitycatalog --values values.yaml
```

## Upgrade and uninstall

To upgrade, read the release notes, then run `helm upgrade` with the new `--version`.

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
