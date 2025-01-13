package io.unitycatalog.server.base.tempcredential;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTestWithMockCredentials;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTest;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.*;

public abstract class BaseTemporaryTableCredentialTest extends BaseCRUDTestWithMockCredentials {
  protected TemporaryCredentialOperations temporaryCredentialOperations;
  protected TableOperations tableOperations;
  protected SchemaOperations schemaOperations;

  protected abstract TemporaryCredentialOperations createTemporaryCredentialsOperations(
      ServerConfig serverConfig);

  protected abstract TableOperations createTableOperations(ServerConfig serverConfig);

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

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

    SchemaInfo schemaInfo =
        schemaOperations.createSchema(
            new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
  }


  @ParameterizedTest
  @ValueSource(strings = {"s3", "abfs", "gs"})
  public void testGenerateTemporaryTableCredentials(String scheme) throws ApiException, IOException {
    createCatalogAndSchema();
    String url = "";
    // test-bucket0 is configured in server properties
    switch (scheme) {
      case "s3" -> url = "s3://test-bucket0/test";
      case "abfs" -> url = "abfs://test-container@test-bucket0.dfs.core.windows.net/test";
      case "gs" -> url = "gs://test-bucket0/test";
      default -> fail("Invalid scheme");
    }

    URI uri = URI.create(url);
    String tableName = "testtable-" + uri.getScheme();
    TableInfo tableInfo =
        BaseTableCRUDTest.createTestingTable(tableName, url + "/" + tableName, tableOperations);

    GenerateTemporaryTableCredential generateTemporaryTableCredential =
        new GenerateTemporaryTableCredential()
            .tableId(tableInfo.getTableId())
            .operation(TableOperation.READ);
      TemporaryCredentials temporaryCredentials =
          temporaryCredentialOperations.generateTemporaryTableCredentials(
              generateTemporaryTableCredential);
    switch (scheme) {
      case "s3":
        assertThat(temporaryCredentials.getAwsTempCredentials()).isNotNull();
        assertThat(temporaryCredentials.getAwsTempCredentials().getSessionToken())
                .isEqualTo("test-session-token");
        assertThat(temporaryCredentials.getAwsTempCredentials().getAccessKeyId())
                .isEqualTo("test-access-key-id");
        assertThat(temporaryCredentials.getAwsTempCredentials().getSecretAccessKey())
                .isEqualTo("test-secret-access-key");
        break;
      case "abfs":
        assertThat(temporaryCredentials.getAzureUserDelegationSas()).isNotNull();
        assertThat(temporaryCredentials.getAzureUserDelegationSas().getSasToken())
                .isEqualTo("test-sas-token");
        break;
      case "gs":
        assertThat(temporaryCredentials.getGcpOauthToken()).isNotNull();
        assertThat(temporaryCredentials.getGcpOauthToken().getOauthToken())
                .isEqualTo("test-token");
        break;
      default:
        fail("Invalid scheme");
        break;
    }
  }
}
