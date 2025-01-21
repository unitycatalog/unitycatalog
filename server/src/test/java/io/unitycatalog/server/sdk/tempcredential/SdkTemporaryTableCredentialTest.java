package io.unitycatalog.server.sdk.tempcredential;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTestWithMockCredentials;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTest;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class SdkTemporaryTableCredentialTest extends BaseCRUDTestWithMockCredentials {
  protected SdkTemporaryCredentialOperations temporaryCredentialOperations;
  protected TableOperations tableOperations;
  protected SchemaOperations schemaOperations;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  protected SdkTemporaryCredentialOperations createTemporaryCredentialsOperations(
      ServerConfig serverConfig) {
    return new SdkTemporaryCredentialOperations(TestUtils.createApiClient(serverConfig));
  }

  protected TableOperations createTableOperations(ServerConfig serverConfig) {
    return new SdkTableOperations(TestUtils.createApiClient(serverConfig));
  }

  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    temporaryCredentialOperations = createTemporaryCredentialsOperations(serverConfig);
    tableOperations = createTableOperations(serverConfig);
    schemaOperations = createSchemaOperations(serverConfig);
  }

  protected void createCatalogAndSchema() throws ApiException {
    CreateCatalog createCatalog =
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT);
    catalogOperations.createCatalog(createCatalog);

    schemaOperations.createSchema(
        new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
  }

  @ParameterizedTest
  @MethodSource("provideTestArguments")
  public void testGenerateTemporaryCredentialsWhereConfIsProvided(
      String scheme, boolean isConfiguredPath) throws ApiException, IOException {
    createCatalogAndSchema();
    String url = getTestCloudPath(scheme, isConfiguredPath);
    URI uri = URI.create(url);
    String tableName = "testtable-" + uri.getScheme();
    TableInfo tableInfo =
        BaseTableCRUDTest.createTestingTable(tableName, url + "/" + tableName, tableOperations);

    GenerateTemporaryTableCredential generateTemporaryTableCredential =
        new GenerateTemporaryTableCredential()
            .tableId(tableInfo.getTableId())
            .operation(TableOperation.READ);
    if (isConfiguredPath) {
      TemporaryCredentials temporaryCredentials =
          temporaryCredentialOperations.generateTemporaryTableCredentials(
              generateTemporaryTableCredential);
      assertTemporaryCredentials(temporaryCredentials, scheme);
    } else {
      assertThatThrownBy(
              () ->
                  temporaryCredentialOperations.generateTemporaryTableCredentials(
                      generateTemporaryTableCredential))
          .isInstanceOf(ApiException.class);
    }
  }
}
