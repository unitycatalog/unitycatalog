package io.unitycatalog.cli.function;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.cli.TestUtils;
import io.unitycatalog.cli.catalog.CliCatalogOperations;
import io.unitycatalog.cli.schema.CliSchemaOperations;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.function.BaseFunctionCRUDTest;
import io.unitycatalog.server.base.function.FunctionOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import java.util.List;
import org.junit.jupiter.api.Test;

public class CliFunctionCRUDTest extends BaseFunctionCRUDTest {
  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new CliCatalogOperations(serverConfig);
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new CliSchemaOperations(serverConfig);
  }

  @Override
  protected FunctionOperations createFunctionOperations(ServerConfig serverConfig) {
    return new CliFunctionOperations(serverConfig);
  }

  @Test
  public void testFunctionCallErrorShowsThePythonError() throws ApiException {
    createCommonResources();
    String fullName = CATALOG_NAME + "." + SCHEMA_NAME + ".raise_error";
    TestUtils.executeCliCommand(
        serverConfig,
        List.of(
            "function",
            "create",
            "--full_name",
            fullName,
            "--input_params",
            "a INT",
            "--data_type",
            "INT",
            "--def",
            "raise ValueError('a must be positive')"));

    assertThatThrownBy(
            () ->
                TestUtils.executeCliCommand(
                    serverConfig,
                    List.of("function", "call", "--full_name", fullName, "--input_params", "5")))
        .isInstanceOf(CliException.class)
        .hasMessageStartingWith(
            "Failed to execute function "
                + fullName
                + ": Python function failed with exit code 1: ")
        .hasMessageContaining("ValueError: a must be positive");
  }
}
