package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.server.base.BaseCRUDTestWithMockCredentials;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.CooperativeDeadline;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.LocalMappingFileOperations;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Credential-vending integration for the Iceberg REST catalog, driven through the hand-rolled
 * {@link IcebergRestClient}. Extends {@link BaseCRUDTestWithMockCredentials} so the server runs
 * with mock cloud-credential vendors, and decorates {@link FileOperations} with {@link
 * LocalMappingFileOperations} so a cloud-rooted catalog's file IO runs on the local filesystem
 * while still requiring the credentials UC vends. {@code IcebergRestCatalogTest} covers the REST
 * surface; this focuses on the vended-credential path, which needs the mock-credential harness.
 */
public class IcebergRestCatalogCredentialVendingTest extends BaseCRUDTestWithMockCredentials {

  private static final String CLOUD_CATALOG = "uc_iceberg_cloud";

  private IcebergRestClient icebergClient;
  // Maps a registered cloud prefix to a local dir; kept so a test can register its catalog root and
  // resolve cloud locations back to local paths.
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
    // Native Iceberg REST writes are opt-in in production; this integration test exercises them.
    serverProperties.setProperty(Property.ICEBERG_TABLE_ENABLED.getKey(), "true");
  }

  @Override
  protected FileOperations decorateFileOperations(FileOperations fileOperations) {
    // Map a registered cloud prefix to local files so the cloud file-IO path can run without a real
    // backend, validating UC vended the expected credentials before any access. The root is
    // registered after the server starts, so keep the instance.
    mappingFileOperations =
        new LocalMappingFileOperations(fileOperations, EXPECTED_VENDED_S3_CREDENTIALS);
    return mappingFileOperations;
  }

  @BeforeEach
  @Override
  public void setUp() {
    // Creates the default (file://) catalog + schema and the mock cloud-credential vendors.
    super.setUp();
    icebergClient = new IcebergRestClient(serverConfig);
  }

  /**
   * Registers a cloud-storage mapping and creates a catalog rooted there with the {@code
   * SCHEMA_NAME} namespace.
   */
  @SneakyThrows
  private void createCloudCatalog(String catalog, String cloudRoot, Path localDir) {
    mappingFileOperations.mapLocation(NormalizedURL.from(cloudRoot), localDir);
    catalogOperations.createCatalog(new CreateCatalog().name(catalog).storageRoot(cloudRoot));
    icebergClient.createNamespace(catalog, TestUtils.SCHEMA_NAME);
  }

  private Path localPathOf(String cloudLocation) {
    return mappingFileOperations.localPathOf(NormalizedURL.from(cloudLocation));
  }

  /**
   * The cloud schemes to vend for, each with the config key and expected value UC returns for it:
   * s3 vends static credentials through the real {@code AwsCredentialVendor} (access key pinned to
   * {@code S3_ACCESS_KEY}); gs vends an OAuth token through the mock GCS vendor.
   */
  private static Stream<Arguments> cloudCredentialCases() {
    return Stream.of(
        Arguments.of("s3", S3FileIOProperties.ACCESS_KEY_ID, S3_ACCESS_KEY),
        Arguments.of("gs", GCPProperties.GCS_OAUTH2_TOKEN, GCS_OAUTH_TOKEN));
  }

  /**
   * Exercises every credential-vending path against a single cloud-rooted table for one scheme:
   * create (write), load (read), {@code /credentials} (refresh), and cleanup (last, since it
   * deletes). Each runs against its own server, so the one-mapping-per-instance limit is respected.
   * Throughout, the fake FileIO requires the credential UC vends before it touches the mapped local
   * storage, and each REST response is checked to carry that same credential.
   */
  @ParameterizedTest
  @MethodSource("cloudCredentialCases")
  public void testCredentialVending(String scheme, String credentialKey, String credentialValue)
      throws Exception {
    String bucketPrefix = scheme + "://" + CONFIGURED_BUCKET;
    String subdir = scheme + "catalog";
    String root = bucketPrefix + testDirectoryRoot.toAbsolutePath() + "/" + subdir;
    createCloudCatalog(CLOUD_CATALOG, root, testDirectoryRoot.toAbsolutePath().resolve(subdir));
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    // createTable (write path): vends and validates the credential, writes metadata to the mapped
    // local dir, and returns the credential plus the cloud metadata location.
    LoadTableResponse created =
        icebergClient.createTable(
            CLOUD_CATALOG,
            TestUtils.SCHEMA_NAME,
            CreateTableRequest.builder()
                .withName(TestUtils.TABLE_NAME)
                .withSchema(schema)
                .withLocation(root + "/ext_iceberg")
                .build());
    String metadata = created.tableMetadata().metadataFileLocation();
    assertThat(metadata).startsWith(bucketPrefix).contains("/metadata/00000-");
    assertThat(Files.exists(localPathOf(metadata))).isTrue();
    assertThat(created.config()).containsEntry(credentialKey, credentialValue);

    // loadTable (read path): reads the metadata back through the fake FileIO (which requires the
    // vended credential) and hands the client the same credential.
    LoadTableResponse loaded =
        icebergClient.loadTable(CLOUD_CATALOG, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
    assertThat(loaded.tableMetadata().metadataFileLocation()).startsWith(bucketPrefix);
    assertThat(loaded.config()).containsEntry(credentialKey, credentialValue);

    // /credentials (refresh path): vends the credential straight to the client, scoped to the
    // table.
    LoadCredentialsResponse response =
        icebergClient.loadCredentials(CLOUD_CATALOG, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
    assertThat(response.credentials()).hasSize(1);
    Credential credential = response.credentials().get(0);
    assertThat(credential.prefix()).startsWith(bucketPrefix).contains("/ext_iceberg");
    assertThat(credential.config()).containsEntry(credentialKey, credentialValue);

    // Storage cleanup (last, since it deletes): the decorator vends and validates read/write
    // credentials and rewrites the cloud prefix to the mapped local dir, so the metadata is visible
    // through the cloud location and then removable.
    String tableLocation = created.tableMetadata().location();
    String prefix = tableLocation + "/";
    try (SupportsPrefixOperations cleanup =
        mappingFileOperations.getCleanupFileIO(
            NormalizedURL.from(tableLocation), CooperativeDeadline.NO_DEADLINE)) {
      assertThat(cleanup.listPrefix(prefix).iterator().hasNext()).isTrue();
      cleanup.deletePrefix(prefix);
      assertThat(cleanup.listPrefix(prefix).iterator().hasNext()).isFalse();
    }
  }
}
