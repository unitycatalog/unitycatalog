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
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.S3AssumeRole;
import io.unitycatalog.client.model.S3CompatibleRequest;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
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
import org.junit.jupiter.api.Test;
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

  /** Creates an external table and an object in its directory, written with the storage's key. */
  private TableInfo createTableWithFile(String name) {
    TableInfo table =
        BaseTableCRUDTestEnv.createTestingTable(
            name,
            TableType.EXTERNAL,
            Optional.of("s3://" + BUCKET + "/" + tablesRoot + "/" + name),
            new SdkTableOperations(TestUtils.createApiClient(serverConfig)));
    try (S3Client client =
        s3Client(
            storage.s3Endpoint(),
            null,
            AwsBasicCredentials.create(storage.accessKeyId(), storage.secretAccessKey()))) {
      client.putObject(b -> b.bucket(BUCKET).key(keyOf(name, FILE)), RequestBody.fromString("x"));
    }
    return table;
  }

  private String keyOf(String table, String file) {
    return tablesRoot + "/" + table + "/" + file;
  }

  private AwsCredentials vend(TableInfo table, TableOperation operation) throws ApiException {
    AwsCredentials vended =
        temporaryCredentialsApi
            .generateTemporaryTableCredentials(
                new GenerateTemporaryTableCredential()
                    .tableId(table.getTableId())
                    .operation(operation))
            .getAwsTempCredentials();
    // A session, not the storage user's own key.
    assertThat(vended.getSessionToken()).isNotBlank();
    assertThat(vended.getAccessKeyId()).isNotEqualTo(storage.accessKeyId());
    assertThat(vended.getS3Endpoint()).isEqualTo(storage.s3Endpoint());
    return vended;
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

  @Test
  public void testTableCredentialReachesOnlyItsTable() throws ApiException {
    TableInfo table = createTableWithFile("orders");
    createTableWithFile("customers");
    // Shares the table's name as a string prefix, so a policy missing the trailing "/" would let
    // the table's credential reach it.
    createTableWithFile("orders_backup");

    try (S3Client client = sessionClient(vend(table, TableOperation.READ_WRITE))) {
      // The table's own objects: read, list, write.
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

      // A sibling table, a table named with this table's name as a prefix, and the bucket root.
      for (String other : List.of("customers", "orders_backup")) {
        assertDenied(c -> c.getObject(b -> b.bucket(BUCKET).key(keyOf(other, FILE))), client);
        assertDenied(
            c ->
                c.putObject(
                    b -> b.bucket(BUCKET).key(keyOf(other, "part-1.parquet")),
                    RequestBody.fromString("y")),
            client);
        assertDenied(
            c -> c.listObjectsV2(b -> b.bucket(BUCKET).prefix(tablesRoot + "/" + other + "/")),
            client);
      }
      assertDenied(c -> c.listObjectsV2(b -> b.bucket(BUCKET)), client);
    }
  }

  @Test
  public void testReadCredentialCannotWrite() throws ApiException {
    TableInfo table = createTableWithFile("events");

    try (S3Client client = sessionClient(vend(table, TableOperation.READ))) {
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
  }
}
