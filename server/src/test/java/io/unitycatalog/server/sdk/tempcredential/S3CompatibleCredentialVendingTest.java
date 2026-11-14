package io.unitycatalog.server.sdk.tempcredential;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.ModelVersionsApi;
import io.unitycatalog.client.api.RegisteredModelsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.DeltaCreateStagingTableRequest;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaStagingTableResponse;
import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CreateModelVersion;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.GenerateTemporaryModelVersionCredential;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.GenerateTemporaryVolumeCredential;
import io.unitycatalog.client.model.ModelVersionInfo;
import io.unitycatalog.client.model.ModelVersionOperation;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.S3AssumeRole;
import io.unitycatalog.client.model.S3CompatibleRequest;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeOperation;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.teststorage.TestStorage;
import io.unitycatalog.server.teststorage.TestStorages;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.ServerProperties.S3CompatibleStaticKeyVending;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Checks what every client credential endpoint returns for a path behind an {@code s3_compatible}
 * credential on real S3-compatible storage. Without {@code assume_role}, {@code
 * server.s3-compatible-storage.static-key-vending} decides: {@code endpoint-only} returns the
 * endpoint and region with no keys, and {@code static-key} the key pair. With {@code assume_role},
 * every endpoint returns an STS session whatever the mode; the test runs those storages only in
 * {@code static-key} mode, where a vended key pair in place of a session would show. Runs only when
 * {@code UC_TEST_STORAGE} selects such storage, since the server writes Iceberg metadata to it.
 */
public class S3CompatibleCredentialVendingTest extends BaseCRUDTest {
  private static final String BUCKET = "vending-bucket";

  private TestStorage storage;
  private String root;
  private ApiClient apiClient;
  private TemporaryCredentialsApi temporaryCredentialsApi;
  private DeltaTemporaryCredentialsApi deltaCredentialsApi;
  private IcebergRestClient icebergClient;

  @BeforeAll
  public static void requireS3CompatibleStorage() {
    Assumptions.assumeTrue(
        TestStorages.selected().isPresent(), "Needs S3-compatible storage (UC_TEST_STORAGE)");
  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    TestStorage selected = TestStorages.selected().orElseThrow();
    S3CompatibleStaticKeyVending mode =
        selected.roleArn().isPresent()
            ? S3CompatibleStaticKeyVending.STATIC_KEY
            : selected.staticKeyVending();
    serverProperties.setProperty(Property.S3_COMPATIBLE_STATIC_KEY_VENDING.getKey(), mode.value());
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    storage = TestStorages.selected().orElseThrow();
    storage.createBucket(BUCKET);
    root = "s3://" + BUCKET + "/run-" + UUID.randomUUID();
    apiClient = TestUtils.createApiClient(serverConfig);
    temporaryCredentialsApi = new TemporaryCredentialsApi(apiClient);
    deltaCredentialsApi = new DeltaTemporaryCredentialsApi(apiClient);
    icebergClient = new IcebergRestClient(serverConfig);
    try {
      new CredentialsApi(apiClient)
          .createCredential(
              new CreateCredentialRequest()
                  .name("vending_credential")
                  .purpose(CredentialPurpose.STORAGE)
                  .s3Compatible(
                      new S3CompatibleRequest()
                          .accessKeyId(storage.accessKeyId())
                          .secretAccessKey(storage.secretAccessKey())
                          .s3Endpoint(storage.s3Endpoint())
                          .assumeRole(
                              storage
                                  .roleArn()
                                  .map(arn -> new S3AssumeRole().roleArn(arn))
                                  .orElse(null))));
      new ExternalLocationsApi(apiClient)
          .createExternalLocation(
              new CreateExternalLocation()
                  .name("vending_location")
                  .url("s3://" + BUCKET)
                  .credentialName("vending_credential"));
      catalogOperations.createCatalog(
          new CreateCatalog().name(CATALOG_NAME).storageRoot(root + "/managed"));
      new SchemasApi(apiClient)
          .createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testEveryEndpointVendsPerCredentialAndMode() throws ApiException {
    // UC REST temporary credentials.
    TableInfo table =
        BaseTableCRUDTestEnv.createTestingTable(
            "external_table",
            TableType.EXTERNAL,
            Optional.of(root + "/external_table"),
            new SdkTableOperations(apiClient));
    assertAwsCredentials(
        temporaryCredentialsApi.generateTemporaryTableCredentials(
            new GenerateTemporaryTableCredential()
                .tableId(table.getTableId())
                .operation(TableOperation.READ_WRITE)));
    assertAwsCredentials(
        temporaryCredentialsApi.generateTemporaryPathCredentials(
            new GenerateTemporaryPathCredential()
                .url(root + "/path")
                .operation(PathOperation.PATH_READ_WRITE)));
    VolumeInfo volume =
        new VolumesApi(apiClient)
            .createVolume(
                new CreateVolumeRequestContent()
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)
                    .name("external_volume")
                    .volumeType(VolumeType.EXTERNAL)
                    .storageLocation(root + "/external_volume"));
    assertAwsCredentials(
        temporaryCredentialsApi.generateTemporaryVolumeCredentials(
            new GenerateTemporaryVolumeCredential()
                .volumeId(volume.getVolumeId())
                .operation(VolumeOperation.WRITE_VOLUME)));
    new RegisteredModelsApi(apiClient)
        .createRegisteredModel(
            new CreateRegisteredModel()
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .name("model"));
    ModelVersionInfo modelVersion =
        new ModelVersionsApi(apiClient)
            .createModelVersion(
                new CreateModelVersion()
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)
                    .modelName("model")
                    .source("source"));
    assertAwsCredentials(
        temporaryCredentialsApi.generateTemporaryModelVersionCredentials(
            new GenerateTemporaryModelVersionCredential()
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .modelName("model")
                .version(modelVersion.getVersion())
                .operation(ModelVersionOperation.READ_WRITE_MODEL_VERSION)));

    // UC Delta API.
    assertDeltaCredentials(
        deltaCredentialsApi
            .getTableCredentials(
                DeltaCredentialOperation.READ_WRITE, CATALOG_NAME, SCHEMA_NAME, table.getName())
            .getStorageCredentials());
    DeltaStagingTableResponse staging =
        new DeltaTablesApi(apiClient)
            .createStagingTable(
                CATALOG_NAME,
                SCHEMA_NAME,
                new DeltaCreateStagingTableRequest().name("staging_table"));
    assertDeltaCredentials(staging.getStorageCredentials());
    assertDeltaCredentials(
        deltaCredentialsApi
            .getStagingTableCredentials(staging.getTableId())
            .getStorageCredentials());

    // Iceberg REST catalog: the server writes and reads the metadata with the key itself.
    icebergClient.createNamespace(CATALOG_NAME, "iceberg");
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    LoadTableResponse created =
        icebergClient.createTable(
            CATALOG_NAME,
            "iceberg",
            CreateTableRequest.builder().withName("iceberg_table").withSchema(schema).build());
    assertIcebergConfig(created.config());
    assertIcebergConfig(icebergClient.loadTable(CATALOG_NAME, "iceberg", "iceberg_table").config());
    List<Credential> credentials =
        icebergClient.loadCredentials(CATALOG_NAME, "iceberg", "iceberg_table").credentials();
    assertThat(credentials).singleElement().satisfies(c -> assertIcebergConfig(c.config()));
    LoadTableResponse staged =
        icebergClient.createTable(
            CATALOG_NAME,
            "iceberg",
            CreateTableRequest.builder()
                .withName("staged_table")
                .withSchema(schema)
                .stageCreate()
                .build());
    assertIcebergConfig(staged.config());
  }

  private void assertAwsCredentials(TemporaryCredentials credentials) {
    AwsCredentials aws = credentials.getAwsTempCredentials();
    assertThat(aws).isNotNull();
    assertThat(aws.getS3Endpoint()).isEqualTo(storage.s3Endpoint());
    assertThat(aws.getS3Region()).isNotBlank();
    assertKeys(aws.getAccessKeyId(), aws.getSecretAccessKey(), aws.getSessionToken());
    assertExpiration(credentials.getExpirationTime());
  }

  private void assertDeltaCredentials(List<DeltaStorageCredential> credentials) {
    assertThat(credentials)
        .singleElement()
        .satisfies(
            credential -> {
              DeltaStorageCredentialConfig config = credential.getConfig();
              assertThat(config.getS3Endpoint()).isEqualTo(storage.s3Endpoint());
              assertThat(config.getClientRegion()).isNotBlank();
              assertKeys(
                  config.getS3AccessKeyId(),
                  config.getS3SecretAccessKey(),
                  config.getS3SessionToken());
              assertExpiration(credential.getExpirationTimeMs());
            });
  }

  private void assertIcebergConfig(Map<String, String> config) {
    assertThat(config)
        .containsEntry(S3FileIOProperties.ENDPOINT, storage.s3Endpoint())
        .containsEntry(S3FileIOProperties.PATH_STYLE_ACCESS, "true")
        .containsKey(AwsClientProperties.CLIENT_REGION);
    assertKeys(
        config.get(S3FileIOProperties.ACCESS_KEY_ID),
        config.get(S3FileIOProperties.SECRET_ACCESS_KEY),
        config.get(S3FileIOProperties.SESSION_TOKEN));
    // Iceberg refreshes only sessions, so only an STS session comes with its expiry.
    if (storage.roleArn().isPresent()) {
      assertThat(config).containsKey(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS);
    } else {
      assertThat(config).doesNotContainKey(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS);
    }
  }

  /** Every vend to a client, with keys or only the endpoint, says when to fetch again. */
  private void assertExpiration(Long expirationTimeMs) {
    assertThat(expirationTimeMs).isNotNull().isGreaterThan(System.currentTimeMillis());
  }

  /**
   * An STS session with {@code assume_role}; otherwise the storage user's key pair in {@code
   * static-key} mode, and no keys in {@code endpoint-only} mode.
   */
  private void assertKeys(String accessKeyId, String secretAccessKey, String sessionToken) {
    if (storage.roleArn().isPresent()) {
      assertThat(accessKeyId).isNotBlank().isNotEqualTo(storage.accessKeyId());
      assertThat(secretAccessKey).isNotBlank();
      assertThat(sessionToken).isNotBlank();
    } else if (storage.vendsKeys()) {
      assertThat(accessKeyId).isEqualTo(storage.accessKeyId());
      assertThat(secretAccessKey).isEqualTo(storage.secretAccessKey());
      assertThat(sessionToken).isNull();
    } else {
      assertThat(accessKeyId).isNull();
      assertThat(secretAccessKey).isNull();
      assertThat(sessionToken).isNull();
    }
  }
}
