package io.unitycatalog.server.base.metastore;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.GetMetastoreSummaryResponse;

public interface MetastoreOperations {
  GetMetastoreSummaryResponse getMetastoreSummary() throws ApiException;
}
