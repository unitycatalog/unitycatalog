# Unity Catalog Helm Chart

A Helm chart for deploying the [UnityCatalog](https://github.com/unitycatalog/unitycatalog) application on a Kubernetes cluster.

## Prerequisites

- Kubernetes 1.12+
- Helm 3.0+

## Installation

To install the chart with the release name `unitycatalog`:

```sh
helm install unitycatalog ./unitycatalog
```

The command deploys the Unity Catalog on the Kubernetes cluster using the default configuration. The [parameters](#parameters) section lists the parameters that can be configured during installation.

## Uninstallation

To uninstall/delete the `unitycatalog` deployment:

```sh
helm uninstall unitycatalog
```

The command removes all the Kubernetes components associated with the chart and deletes the release.

## Parameters

The following table lists the configurable parameters of the UnityCatalog chart and their default values.

| Parameter                | Description                                      | Default                        |
|--------------------------|--------------------------------------------------|--------------------------------|
| `replicaCount`           | Number of replicas for the deployment            | `1`                            |
| `image.repository`       | Image repository                                 | `unitycatalog/unitycatalog`    |
| `image.tag`              | Image tag                                        | `latest`                       |
| `image.pullPolicy`       | Image pull policy                                | `IfNotPresent`                 |
| `service.type`           | Kubernetes service type                          | `ClusterIP`                    |
| `service.port`           | Service port                                     | `8081`                         |
| `ingress.enabled`        | Enable ingress controller resource               | `false`                        |
| `ingress.annotations`    | Ingress annotations                              | `{}`                           |
| `ingress.hosts`          | Hostnames for ingress                            | `[]`                           |
| `resources`              | Resource requests and limits                     | `{}`                           |
| `nodeSelector`           | Node labels for pod assignment                   | `{}`                           |
| `tolerations`            | Tolerations for pod assignment                   | `[]`                           |
| `affinity`               | Affinity settings for pod assignment             | `{}`                           |

## Example

To deploy Unity Catalog with a specific configuration:

```sh
helm install unitycatalog ./unitycatalog --set replicaCount=2 --set service.type=LoadBalancer
```

## License

This project is licensed under the Apache License. See the [LICENSE](https://github.com/unitycatalog/unitycatalog/blob/main/LICENSE) file for details.
