package io.unitycatalog.cli.metastore;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.model.GetMetastoreSummaryResponse;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.metastore.MetastoreOperations;
import java.util.List;

public class CliMetastoreOperations implements MetastoreOperations {
  private final ServerConfig config;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public CliMetastoreOperations(ServerConfig config) {
    this.config = config;
  }

  @Override
  public GetMetastoreSummaryResponse getMetastoreSummary() {
    String[] args = addServerAndAuthParams(List.of("metastore", "get"), config);
    JsonNode metastoreSummaryJson = executeCLICommand(args);
    return objectMapper.convertValue(metastoreSummaryJson, GetMetastoreSummaryResponse.class);
  }
}
