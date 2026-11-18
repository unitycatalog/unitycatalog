package io.unitycatalog.server.sdk.tempcredential;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.GenerateTemporaryVolumeCredential;
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
import io.unitycatalog.server.utils.TestUtils;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Checks on real S3-compatible storage that table credentials Unity Catalog vends reach only that
 * table: the storage, not Unity Catalog, enforces the session policy of the STS session. Runs only
 * when {@code UC_TEST_STORAGE} selects such storage, since emulated storage enforces nothing.
 */
public class S3CompatibleCredentialScopeTest extends BaseCRUDTest {
  private static final String BUCKET = "scope-bucket";
  private static final String FILE = "part-0.parquet";

  private TestStorage storage;
  private TemporaryCredentialsApi temporaryCredentialsApi;
  // Each test writes under its own prefix, so tests never see each other's objects.
  private String tablesRoot;

  @BeforeAll
  public static void requireS3CompatibleStorage() {
    Assumptions.assumeTrue(
        TestStorages.selected().isPresent(),
        "Needs S3-compatible storage that enforces session policies (UC_TEST_STORAGE)");
    Assumptions.assumeTrue(
        TestStorages.selected().orElseThrow().enforcesSessionPolicies(),
        "The selected storage vends no AssumeRole sessions narrowed by a session policy, so vended"
            + " credentials reach everything the storage user reaches");
  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    enableS3CompatibleStorage();
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    storage = TestStorages.selected().orElseThrow();
    storage.createBucket(BUCKET);
    tablesRoot = "run-" + UUID.randomUUID() + "/tables";
    ApiClient apiClient = TestUtils.createApiClient(serverConfig);
    temporaryCredentialsApi = new TemporaryCredentialsApi(apiClient);
    try {
      new CredentialsApi(apiClient)
          .createCredential(
              new CreateCredentialRequest()
                  .name("scope_credential")
                  .purpose(CredentialPurpose.STORAGE)
                  .s3Compatible(
                      new S3CompatibleRequest()
                          .accessKeyId(storage.accessKeyId())
                          .secretAccessKey(storage.secretAccessKey())
                          .s3Endpoint(storage.s3Endpoint())
                          .assumeRole(
                              new S3AssumeRole().roleArn(storage.roleArn().orElseThrow()))));
      new ExternalLocationsApi(apiClient)
          .createExternalLocation(
              new CreateExternalLocation()
                  .name("scope_location")
                  .url("s3://" + BUCKET)
                  .credentialName("scope_credential"));
      catalogOperations.createCatalog(new CreateCatalog().name(CATALOG_NAME));
      new SchemasApi(apiClient)
          .createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  /** Writes an object in the directory {@code name}, with the storage's own key. */
  private void createDirectoryWithFile(String name) {
    try (S3Client client =
        s3Client(
            storage.s3Endpoint(),
            null,
            AwsBasicCredentials.create(storage.accessKeyId(), storage.secretAccessKey()))) {
      client.putObject(b -> b.bucket(BUCKET).key(keyOf(name, FILE)), RequestBody.fromString("x"));
    }
  }

  private String urlOf(String name) {
    return "s3://" + BUCKET + "/" + tablesRoot + "/" + name;
  }

  /** A securable that credentials are vended for, over one directory. */
  enum Securable {
    TABLE,
    PATH,
    VOLUME
  }

  /**
   * Vends credentials for {@code securable} over the directory {@code name}, creating the table or
   * volume there first.
   */
  private AwsCredentials vend(Securable securable, String name, boolean isWrite)
      throws ApiException {
    ApiClient apiClient = TestUtils.createApiClient(serverConfig);
    TemporaryCredentials vended =
        switch (securable) {
          case TABLE -> {
            TableInfo table =
                BaseTableCRUDTestEnv.createTestingTable(
                    name,
                    TableType.EXTERNAL,
                    Optional.of(urlOf(name)),
                    new SdkTableOperations(apiClient));
            yield temporaryCredentialsApi.generateTemporaryTableCredentials(
                new GenerateTemporaryTableCredential()
                    .tableId(table.getTableId())
                    .operation(isWrite ? TableOperation.READ_WRITE : TableOperation.READ));
          }
          case PATH ->
              temporaryCredentialsApi.generateTemporaryPathCredentials(
                  new GenerateTemporaryPathCredential()
                      .url(urlOf(name))
                      .operation(
                          isWrite ? PathOperation.PATH_READ_WRITE : PathOperation.PATH_READ));
          case VOLUME -> {
            VolumeInfo volume =
                new VolumesApi(apiClient)
                    .createVolume(
                        new CreateVolumeRequestContent()
                            .catalogName(CATALOG_NAME)
                            .schemaName(SCHEMA_NAME)
                            .name(name)
                            .volumeType(VolumeType.EXTERNAL)
                            .storageLocation(urlOf(name)));
            yield temporaryCredentialsApi.generateTemporaryVolumeCredentials(
                new GenerateTemporaryVolumeCredential()
                    .volumeId(volume.getVolumeId())
                    .operation(
                        isWrite ? VolumeOperation.WRITE_VOLUME : VolumeOperation.READ_VOLUME));
          }
        };
    AwsCredentials aws = vended.getAwsTempCredentials();
    // A session, not the storage user's own key.
    assertThat(aws.getSessionToken()).isNotBlank();
    assertThat(aws.getAccessKeyId()).isNotEqualTo(storage.accessKeyId());
    assertThat(aws.getS3Endpoint()).isEqualTo(storage.s3Endpoint());
    return aws;
  }

  private String keyOf(String table, String file) {
    return tablesRoot + "/" + table + "/" + file;
  }

  private static S3Client s3Client(
      String endpoint, String region, software.amazon.awssdk.auth.credentials.AwsCredentials key) {
    AwsCredentialsProvider provider = StaticCredentialsProvider.create(key);
    return S3Client.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.of(region == null ? "us-east-1" : region))
        .forcePathStyle(true)
        .credentialsProvider(provider)
        .build();
  }

  private static S3Client sessionClient(AwsCredentials vended) {
    return s3Client(
        vended.getS3Endpoint(),
        vended.getS3Region(),
        AwsSessionCredentials.create(
            vended.getAccessKeyId(), vended.getSecretAccessKey(), vended.getSessionToken()));
  }

  private static void assertDenied(Consumer<S3Client> request, S3Client client) {
    assertThatThrownBy(() -> request.accept(client))
        .isInstanceOf(S3Exception.class)
        .extracting(e -> ((S3Exception) e).statusCode())
        .isEqualTo(403);
  }

  private boolean exists(String key) {
    try (S3Client client =
        s3Client(
            storage.s3Endpoint(),
            null,
            AwsBasicCredentials.create(storage.accessKeyId(), storage.secretAccessKey()))) {
      return !client.listObjectsV2(b -> b.bucket(BUCKET).prefix(key)).contents().isEmpty();
    }
  }

  @ParameterizedTest
  @EnumSource(Securable.class)
  public void testCredentialReachesOnlyItsDirectory(Securable securable) throws ApiException {
    createDirectoryWithFile("orders");
    createDirectoryWithFile("customers");
    // Shares the directory's name as a string prefix, so a policy missing the trailing "/" would
    // let the credential reach it.
    createDirectoryWithFile("orders_backup");

    try (S3Client client = sessionClient(vend(securable, "orders", /* isWrite= */ true))) {
      // Its own objects: read, list, write, delete.
      assertThat(
              client
                  .getObjectAsBytes(b -> b.bucket(BUCKET).key(keyOf("orders", FILE)))
                  .asUtf8String())
          .isEqualTo("x");
      assertThat(
              client
                  .listObjectsV2(b -> b.bucket(BUCKET).prefix(tablesRoot + "/orders/"))
                  .contents())
          .extracting(object -> object.key())
          .contains(keyOf("orders", FILE));
      client.putObject(
          b -> b.bucket(BUCKET).key(keyOf("orders", "part-1.parquet")),
          RequestBody.fromString("y"));
      client.deleteObject(b -> b.bucket(BUCKET).key(keyOf("orders", "part-1.parquet")));

      // A sibling, a directory named with this one's name as a prefix, and the bucket root.
      for (String other : List.of("customers", "orders_backup")) {
        assertDenied(c -> c.getObject(b -> b.bucket(BUCKET).key(keyOf(other, FILE))), client);
        assertDenied(
            c ->
                c.putObject(
                    b -> b.bucket(BUCKET).key(keyOf(other, "part-1.parquet")),
                    RequestBody.fromString("y")),
            client);
        assertDenied(c -> c.deleteObject(b -> b.bucket(BUCKET).key(keyOf(other, FILE))), client);
        assertDenied(
            c -> c.listObjectsV2(b -> b.bucket(BUCKET).prefix(tablesRoot + "/" + other + "/")),
            client);
      }
      assertDenied(c -> c.listObjectsV2(b -> b.bucket(BUCKET)), client);
    }
    // The deleted object is gone, and the others remain.
    assertThat(exists(keyOf("orders", "part-1.parquet"))).isFalse();
    for (String name : List.of("orders", "customers", "orders_backup")) {
      assertThat(exists(keyOf(name, FILE))).as(name).isTrue();
    }
  }

  @ParameterizedTest
  @EnumSource(Securable.class)
  public void testReadCredentialCannotWrite(Securable securable) throws ApiException {
    createDirectoryWithFile("events");

    try (S3Client client = sessionClient(vend(securable, "events", /* isWrite= */ false))) {
      assertThat(
              client
                  .getObjectAsBytes(b -> b.bucket(BUCKET).key(keyOf("events", FILE)))
                  .asUtf8String())
          .isEqualTo("x");
      assertDenied(
          c ->
              c.putObject(
                  b -> b.bucket(BUCKET).key(keyOf("events", "part-1.parquet")),
                  RequestBody.fromString("y")),
          client);
      assertDenied(c -> c.deleteObject(b -> b.bucket(BUCKET).key(keyOf("events", FILE))), client);
    }
    assertThat(exists(keyOf("events", FILE))).isTrue();
  }
}
