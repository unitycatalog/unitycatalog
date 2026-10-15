# Observability

Unity Catalog can expose three unauthenticated HTTP endpoints for health checking and metrics
collection: `/livez`, `/readyz`, and `/metrics`. Observability is **disabled by default**: no extra
listener, Prometheus registry, or background readiness probe is started. To enable it, first
restrict network access as described below, then start the server with `--obs-port <port>`:

```sh
bin/start-uc-server --obs-port 8090
```

The flag requires a port value. Omitting it keeps observability disabled; there is no separate
enable property. The endpoints are served on the **dedicated observability port** you supply,
separate from the catalog API port. In the example above, the client port defaults to `8080`,
the internal API port is `8081`, and the observability port is `8090`.

The observability port is a second port on the *same* server as the API — not a separate server —
so both listeners share one process and fail together. Serving these endpoints on their own port
keeps them (in particular `/metrics`, which exposes operational counters) off the main API
listener: they answer on the observability port and are `404` on the API port. To use another port:

```sh
bin/start-uc-server --port 8080 --obs-port 9464
```

The supplied port must be between `1` and `65535` and differ from both API ports. `0` is not
supported; choose an explicit port. If any required port is already in use, startup fails;
the server does not select another port.

!!! warning "Network exposure"
    These endpoints are unauthenticated by design so that orchestrators and Prometheus
    scrapers can reach them without credentials. Restrict access to the observability port with
    network policy (firewall rules, Kubernetes `NetworkPolicy`, a service mesh, etc.) and do not
    expose it to untrusted networks. `/metrics` in particular exposes operational counts such as
    tables created. Under a default-deny `NetworkPolicy`, remember to allow the observability port
    from the node/host, or kubelet probes will fail regardless of which port they target.

## Health endpoints

### `GET /livez` — liveness

Returns `200 OK` with body `{"healthy":true}` while the process is serving. This check does **not**
touch the database; it answers immediately from the request-handling thread.

Use this endpoint to tell an orchestrator whether the process is alive and should be
restarted if it stops responding.

```sh
curl http://localhost:8090/livez
# {"healthy":true}
```

### `GET /readyz` — readiness

Returns `200 OK` when the catalog database is reachable, or `503 Service Unavailable`
otherwise.

Key implementation details:

- Startup performs one synchronous database check before the server begins serving. Subsequent
  checks run on a background thread; checks are **not** executed on the request path (the endpoint
  returns the cached result of the last check).
- The endpoint **fails closed**: it reports not-ready until the first successful database
  check completes after startup.
- The probe interval and the database check timeout are configurable:
  `server.readiness.probe-interval` (default `PT5S`) and `server.readiness.db-timeout`
  (default `PT2S`). The timeout bounds only the validity check, **not** connection
  acquisition — bound a down or unreachable database with the connection pool's connect
  timeout instead.

Use this endpoint to control routing: add it as the readiness probe and as the health check for
load balancer target groups. If your load balancer health-checks a port it also routes traffic to,
point that health check at `/readyz` on the observability port (load balancers allow a health-check
port distinct from the traffic port) so routing reflects database readiness rather than a static
root response.

```sh
curl -i http://localhost:8090/readyz
# HTTP/1.1 200 OK  (database reachable)
# {"healthy":true}

# HTTP/1.1 503 Service Unavailable  (database unreachable or startup in progress)
# {"healthy":false}
```

## Metrics endpoint

### `GET /metrics` — Prometheus exposition

Returns current metric values in the
[Prometheus text exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/).
Suitable for scraping by Prometheus or any compatible system (Grafana Agent, OpenTelemetry
Collector, etc.).

#### Metric families

| Family | Description |
|---|---|
| `jvm_memory_used_bytes`, `jvm_gc_*`, `jvm_threads_*` | JVM heap and non-heap memory, GC pause, and thread counts |
| `process_cpu_usage`, `system_cpu_usage` | Process and host CPU utilisation |
| `armeria_server_*`, `armeria_executor_*` | Armeria server and executor internals |
| `http_server_requests_total` | Completed HTTP requests on the API listener, tagged by `hostname_pattern`, `service`, `method`, and `http_status`; includes authentication and authorization rejections |
| `uc_securable_table_creations_total` | Table securables created after their database transactions commit through UC, Delta, or Iceberg REST, including tables, views, and metric views |

Within the `http_server_*` family, only `http_server_requests_total` is exposed, without a `result`
tag. HTTP latency, request/response size, timeout, and in-flight metrics are not exposed. JVM,
CPU, Armeria-internal, and UC metrics remain enabled.

#### HTTP request counter labels

- `hostname_pattern`: the API listener's virtual-host pattern, such as `*:8081` with the
  default client port of `8080`. It identifies the internal API listener, not the client or
  observability port.
- `service`: the fully qualified Java service class, such as
  `io.unitycatalog.server.service.CatalogService`.
- `method`: the annotated Java handler name, such as `createCatalog`, **not** the HTTP verb.
  When no annotated handler is selected, the service is
  `com.linecorp.armeria.server.FallbackService` and the method is the HTTP verb, such as `GET`.
- `http_status`: the final HTTP response status, such as `200`, `400`, or `500`.

For example, the following series show a successful catalog creation, a malformed catalog
creation request, a server error during SCIM user creation, and an unmatched request that
returns `404`. These examples use the default internal API port of `8081` and an illustrative
count of one request per series:

```text
http_server_requests_total{hostname_pattern="*:8081",http_status="200",method="createCatalog",service="io.unitycatalog.server.service.CatalogService"} 1.0
http_server_requests_total{hostname_pattern="*:8081",http_status="400",method="createCatalog",service="io.unitycatalog.server.service.CatalogService"} 1.0
http_server_requests_total{hostname_pattern="*:8081",http_status="500",method="createScimUser",service="io.unitycatalog.server.service.Scim2UserService"} 1.0
http_server_requests_total{hostname_pattern="*:8081",http_status="404",method="GET",service="com.linecorp.armeria.server.FallbackService"} 1.0
```

Requests to `/metrics`, `/livez`, and `/readyz` on the observability listener are not included
in this counter. A failure after response headers have been sent cannot change the status
already sent to the client. Without a `result` tag, this counter does not distinguish such a
failure from another request with the same HTTP status.

The persisted-create counter is separate from HTTP request outcomes. For example, a total
of one committed table-securable creation is exposed as:

```text
uc_securable_table_creations_total 1.0
```

!!! note "Per-route metrics are lazily populated"
    HTTP request metrics for a given route (`service`/`method` combination) appear in the
    output only after that route has received its first request. Scrapers should not expect
    zero-valued series for routes that have never been called.

!!! note "Framework metric names track library versions"
    Only the `uc_*` families are owned by Unity Catalog. The `jvm_*`, `process_*`, `system_*`,
    `armeria_*`, and `http_server_*` names come from Micrometer and Armeria and may change when
    those libraries are upgraded; pin dashboards and alerts with that in mind.

```sh
curl http://localhost:8090/metrics
# HELP jvm_memory_used_bytes ...
# TYPE jvm_memory_used_bytes gauge
# jvm_memory_used_bytes{area="heap",...} 1.23456789E8
# ...
```

## Integration examples

### Prometheus scrape configuration

Add a job to your `prometheus.yml` (or equivalent scrape configuration) to collect metrics
from the Unity Catalog server's observability port:

```yaml
scrape_configs:
  - job_name: unity_catalog
    metrics_path: /metrics
    static_configs:
      - targets:
          - uc-server-host:8090
```

Replace `uc-server-host:8090` with the hostname and observability port of your Unity Catalog
server.

### Kubernetes probes

Start the container's server with `--obs-port 8090`, expose that port, and point both probes
at it. Declaring `containerPort` alone does not enable observability:

```yaml
containers:
  - name: unity-catalog
    ports:
      - name: http
        containerPort: 8080
      - name: observability
        containerPort: 8090
    livenessProbe:
      httpGet:
        path: /livez
        port: observability
      initialDelaySeconds: 10
      periodSeconds: 15
    readinessProbe:
      httpGet:
        path: /readyz
        port: observability
      initialDelaySeconds: 5
      periodSeconds: 10
      failureThreshold: 3
```

The liveness probe restarts the container if the process stops serving.
The readiness probe prevents traffic from reaching the container while the database is
unreachable or while startup is still in progress.
