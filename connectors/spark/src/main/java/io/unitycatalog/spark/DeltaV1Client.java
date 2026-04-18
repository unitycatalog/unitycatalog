package io.unitycatalog.spark;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.DeltaV1Api;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.DeltaV1ConfigResponse;
import io.unitycatalog.client.model.DeltaV1CreateStagingTableRequest;
import io.unitycatalog.client.model.DeltaV1CreateTableRequest;
import io.unitycatalog.client.model.DeltaV1LoadTableResponse;
import io.unitycatalog.client.model.DeltaV1StagingTableResponse;
import io.unitycatalog.client.model.DeltaV1UpdateTableRequest;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import java.net.URI;
import java.util.List;
import java.util.Map;

/** Thin adapter around the generated DeltaV1Api used by the Spark connector. */
public class DeltaV1Client {
  private final DeltaV1Api deltaV1Api;
  private DeltaV1ConfigResponse config;

  public DeltaV1Client(URI baseUri, TokenProvider tokenProvider) {
    this.deltaV1Api =
        new DeltaV1Api(
            ApiClientFactory.createApiClient(
                JitterDelayRetryPolicy.builder().build(), baseUri, tokenProvider));
  }

  public DeltaV1ConfigResponse getConfig() {
    ensureConfigured();
    return config;
  }

  public DeltaV1StagingTableResponse createStagingTable(String catalog, String schema, String name)
      throws ApiException {
    ensureConfigured();
    return deltaV1Api.deltaV1CreateStagingTable(
        catalog, schema, new DeltaV1CreateStagingTableRequest().name(name));
  }

  public TemporaryCredentials getStagingTableCredentials(
      String catalog, String schema, String tableId, TableOperation operation) throws ApiException {
    ensureConfigured();
    return deltaV1Api.deltaV1GetStagingTableCredentials(catalog, schema, tableId, operation);
  }

  public DeltaV1LoadTableResponse loadTable(String catalog, String schema, String table)
      throws ApiException {
    ensureConfigured();
    return deltaV1Api.deltaV1LoadTable(catalog, schema, table, null, null);
  }

  public TemporaryCredentials getTableCredentials(
      String catalog, String schema, String table, TableOperation operation) throws ApiException {
    ensureConfigured();
    return deltaV1Api.deltaV1GetTableCredentials(catalog, schema, table, operation);
  }

  public DeltaV1LoadTableResponse createManagedTable(
      String catalog,
      String schema,
      String name,
      String tableId,
      String storageLocation,
      List<ColumnInfo> columns,
      String comment,
      Map<String, String> properties)
      throws ApiException {
    ensureConfigured();
    return deltaV1Api.deltaV1CreateTable(
        catalog,
        schema,
        new DeltaV1CreateTableRequest()
            .name(name)
            .tableId(tableId)
            .storageLocation(storageLocation)
            .columns(columns)
            .comment(comment)
            .properties(properties));
  }

  public DeltaV1LoadTableResponse updateTable(
      String catalog, String schema, String table, DeltaV1UpdateTableRequest request)
      throws ApiException {
    ensureConfigured();
    return deltaV1Api.deltaV1UpdateTable(catalog, schema, table, request);
  }

  private void ensureConfigured() {
    if (config != null) {
      return;
    }
    try {
      config = deltaV1Api.deltaV1Config(null, null);
    } catch (ApiException e) {
      throw new IllegalStateException("Failed to discover delta/v1 API", e);
    }
  }
}
