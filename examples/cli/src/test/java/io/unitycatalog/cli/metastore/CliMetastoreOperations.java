package io.unitycatalog.cli.metastore;

import io.unitycatalog.cli.BaseCliOperations;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.GetMetastoreSummaryResponse;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.metastore.MetastoreOperations;
import java.util.List;

public class CliMetastoreOperations extends BaseCliOperations implements MetastoreOperations {

  public CliMetastoreOperations(ServerConfig config) {
    super("metastore", config);
  }

  @Override
  public GetMetastoreSummaryResponse getMetastoreSummary() throws ApiException {
    return execute(GetMetastoreSummaryResponse.class, "get", List.of());
  }
}
