package io.unitycatalog.server.sdk.metastore;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.MetastoresApi;
import io.unitycatalog.client.model.GetMetastoreSummaryResponse;
import io.unitycatalog.server.base.metastore.MetastoreOperations;

public class SdkMetastoreOperations implements MetastoreOperations {
  private final MetastoresApi metastoresAPI;

  public SdkMetastoreOperations(ApiClient apiClient) {
    this.metastoresAPI = new MetastoresApi(apiClient);
  }

  @Override
  public GetMetastoreSummaryResponse getMetastoreSummary() throws ApiException {
    return metastoresAPI.summary();
  }
}
