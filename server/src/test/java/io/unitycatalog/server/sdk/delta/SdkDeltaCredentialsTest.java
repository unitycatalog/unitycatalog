package io.unitycatalog.server.sdk.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.delta.api.TemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.delta.model.CredentialsResponse;
import io.unitycatalog.client.delta.model.StorageCredential;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
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
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the Delta REST Catalog credential-vending endpoints. One test with
 * sections, to amortize the server + mock-cloud setup cost.
 *
 * <p>Uses {@link BaseCRUDTestWithMockCredentials} which configures mock cloud-credential
 * infrastructure so {@code s3://test-bucket0/...} paths can be vended. Authorization is NOT enabled
 * in this base; authz behavior is covered in {@code SdkTableAccessControlCRUDTest} and {@code
 * SdkStagingTableAccessControlTest}.
 */
public class SdkDeltaCredentialsTest extends BaseCRUDTestWithMockCredentials {

  private TemporaryCredentialsApi deltaCredentialsApi;
  private TableOperations tableOperations;
  private TablesApi tablesApi;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  protected TableOperations createTableOperations(ServerConfig serverConfig) {
    return new SdkTableOperations(TestUtils.createApiClient(serverConfig));
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    var apiClient = TestUtils.createApiClient(serverConfig);
    deltaCredentialsApi = new TemporaryCredentialsApi(apiClient);
    tableOperations = createTableOperations(serverConfig);
    tablesApi = new TablesApi(apiClient);
  }

  @Test
  public void testDeltaCredentialsEndpoints() throws ApiException {
    TableInfo readTable = createExternalS3Table("tbl_read_s3");
    TableInfo readWriteTable = createExternalS3Table("tbl_rw_s3");

    // -------- getTableCredentials: READ (S3) --------
    {
      CredentialsResponse resp =
          deltaCredentialsApi.getTableCredentials(
              CredentialOperation.READ,
              TestUtils.CATALOG_NAME,
              TestUtils.SCHEMA_NAME,
              readTable.getName());
      assertThat(resp.getStorageCredentials()).hasSize(1);
      StorageCredential sc = resp.getStorageCredentials().get(0);
      assertThat(sc.getOperation()).isEqualTo(CredentialOperation.READ);
      assertThat(sc.getPrefix()).startsWith("s3://");
      assertThat(sc.getConfig().getS3AccessKeyId()).isEqualTo("accessKey0");
      assertThat(sc.getConfig().getS3SecretAccessKey()).isNotBlank();
    }

    // -------- getTableCredentials: READ_WRITE (S3) --------
    {
      CredentialsResponse resp =
          deltaCredentialsApi.getTableCredentials(
              CredentialOperation.READ_WRITE,
              TestUtils.CATALOG_NAME,
              TestUtils.SCHEMA_NAME,
              readWriteTable.getName());
      StorageCredential sc = resp.getStorageCredentials().get(0);
      assertThat(sc.getOperation()).isEqualTo(CredentialOperation.READ_WRITE);
      assertThat(sc.getConfig().getS3AccessKeyId()).isNotBlank();
    }

    // -------- getTableCredentials: table not found --------
    assertThatThrownBy(
            () ->
                deltaCredentialsApi.getTableCredentials(
                    CredentialOperation.READ,
                    TestUtils.CATALOG_NAME,
                    TestUtils.SCHEMA_NAME,
                    "nonexistent"))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("not found");

    // -------- getTableCredentials: missing `operation` param (SDK-side required check) --------
    assertThatThrownBy(
            () ->
                deltaCredentialsApi.getTableCredentials(
                    /* operation= */ null,
                    TestUtils.CATALOG_NAME,
                    TestUtils.SCHEMA_NAME,
                    readTable.getName()))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("Missing the required parameter 'operation'");

    // -------- getStagingTableCredentials: happy path --------
    StagingTableInfo staging =
        tablesApi.createStagingTable(
            new CreateStagingTable()
                .catalogName(TestUtils.CATALOG_NAME)
                .schemaName(TestUtils.SCHEMA_NAME)
                .name("staging_for_creds"));
    {
      CredentialsResponse resp =
          deltaCredentialsApi.getStagingTableCredentials(UUID.fromString(staging.getId()));
      assertThat(resp.getStorageCredentials()).hasSize(1);
      StorageCredential sc = resp.getStorageCredentials().get(0);
      // Staging creds are always READ_WRITE for writing the initial commit.
      assertThat(sc.getOperation()).isEqualTo(CredentialOperation.READ_WRITE);
      assertThat(sc.getPrefix()).isEqualTo(staging.getStagingLocation());
    }

    // -------- getStagingTableCredentials: staging UUID not found --------
    UUID randomId = UUID.randomUUID();
    assertThatThrownBy(() -> deltaCredentialsApi.getStagingTableCredentials(randomId))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("Staging table not found");

    // -------- getStagingTableCredentials: rejects regular-table UUID --------
    // Endpoint is scoped to staging tables only; passing a real table's UUID must 404.
    UUID regularTableId = UUID.fromString(readTable.getTableId());
    assertThatThrownBy(() -> deltaCredentialsApi.getStagingTableCredentials(regularTableId))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("Staging table not found");
  }

  /** Creates a configured-S3-backed external table for credential tests. */
  private TableInfo createExternalS3Table(String tableName) {
    String url = getTestCloudPath("s3", /* isConfiguredPath= */ true);
    return BaseTableCRUDTest.createTestingTable(
        tableName, TableType.EXTERNAL, Optional.of(url + "/" + tableName), tableOperations);
  }
}
