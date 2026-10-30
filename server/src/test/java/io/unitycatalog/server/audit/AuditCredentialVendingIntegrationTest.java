package io.unitycatalog.server.audit;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.server.base.BaseCRUDTestWithMockCredentials;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.LocalMappingFileOperations;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Audit records on a cloud (s3) catalog with authorization off: credentials carry no user. */
class AuditCredentialVendingIntegrationTest extends BaseCRUDTestWithMockCredentials {
  private static final String CLOUD_CATALOG = "uc_audit_cloud";
  private LocalMappingFileOperations mappingFileOperations;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.AUDIT_SINK.getKey(), CapturingAuditSink.class.getName());
    CapturingAuditSink.reset();
  }

  @Override
  protected FileOperations decorateFileOperations(FileOperations fileOperations) {
    mappingFileOperations =
        new LocalMappingFileOperations(fileOperations, EXPECTED_VENDED_S3_CREDENTIALS);
    return mappingFileOperations;
  }

  @Test
  void recordsCredentialsWithoutUser() throws Exception {
    String root = "s3://" + CONFIGURED_BUCKET + testDirectoryRoot.toAbsolutePath() + "/cloud";
    mappingFileOperations.mapLocation(
        NormalizedURL.from(root), testDirectoryRoot.toAbsolutePath().resolve("cloud"));
    catalogOperations.createCatalog(new CreateCatalog().name(CLOUD_CATALOG).storageRoot(root));
    IcebergRestClient iceberg = new IcebergRestClient(serverConfig);
    iceberg.createNamespace(CLOUD_CATALOG, TestUtils.SCHEMA_NAME);
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    iceberg.createTable(
        CLOUD_CATALOG,
        TestUtils.SCHEMA_NAME,
        CreateTableRequest.builder().withName(TestUtils.TABLE_NAME).withSchema(schema).build());
    iceberg.loadTable(CLOUD_CATALOG, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);

    assertThat(CapturingAuditSink.RECORDS)
        .isNotEmpty()
        .allSatisfy(
            r -> {
              assertThat(r.getAction()).isEqualTo(AuditRecord.Action.ISSUE_CREDENTIAL); // no grants
              assertThat(r.getUser()).isNull(); // authorization off: no user
              assertThat(r.getLocation()).startsWith("s3://");
              assertThat(r.getRequestId()).isNotBlank(); // joins to the server.log lines
            });
  }

  @Test
  void serverCloseClosesTheSinkOnce() {
    unityCatalogServer.close();
    unityCatalogServer.close();
    assertThat(CapturingAuditSink.CLOSE_COUNT.get()).isEqualTo(1);
  }
}
