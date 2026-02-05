package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.client.model.PathOperation.PATH_CREATE_TABLE;
import static io.unitycatalog.client.model.PathOperation.PATH_READ;
import static io.unitycatalog.client.model.PathOperation.PATH_READ_WRITE;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.TEST_AWS_MASTER_ROLE_ARN;
import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.aws.CredentialsGenerator;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import software.amazon.awssdk.services.sts.model.Credentials;

public class TemporaryPathCredentialAccessControlTest extends SdkAccessControlBaseCRUDTest {

  private static final String TEST_EXTERNAL_LOCATION_NAME = "test_ext_loc";
  private static final String TEST_EXTERNAL_LOCATION_URL = "s3://test-bucket0/path/to/data";
  private static final String TEST_CREDENTIAL_NAME = "test_credential";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";

  private static final String READONLY_EMAIL = "readonly@example.com";
  private static final String READWRITE_EMAIL = "readwrite@example.com";
  private static final String CREATE_TABLE_EMAIL = "createtable@example.com";
  private static final String UNAUTHORIZED_EMAIL = "unauthorized@example.com";
  /** User dedicated to creating external tables and volumes for testing. */
  private static final String TABLE_VOLUME_OWNER_EMAIL = "table_volume_owner@example.com";

  private TemporaryCredentialsApi adminTempCredsApi;

  private TemporaryCredentialsApi locationOwnerTempCredsApi;
  private ApiClient tableVolumeCreatorApiClient;

  @Mock CloudCredentialVendor mockCloudCredentialVendor;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.put(
        ServerProperties.Property.AWS_MASTER_ROLE_ARN.getKey(), TEST_AWS_MASTER_ROLE_ARN);
  }

  // This is access control test. So it doesn't care about the actual credential vending. Just
  // mock with something that returns credential.
  private void setupMockCloudCredentialVendor() {
    // Mock function needs the credential of `server.model` package.
    io.unitycatalog.server.model.TemporaryCredentials credential =
        new io.unitycatalog.server.model.TemporaryCredentials()
            .awsTempCredentials(
                new io.unitycatalog.server.model.AwsCredentials()
                    .accessKeyId("test-access-key-id")
                    .secretAccessKey("test-secret-access-key")
                    .sessionToken("test-session-token"))
            .expirationTime(System.currentTimeMillis() + 6000);
    mockCloudCredentialVendor = mock(CloudCredentialVendor.class);
    doReturn(credential).when(mockCloudCredentialVendor).vendCredential(any());
    cloudCredentialVendor = mockCloudCredentialVendor;
  }

  @SneakyThrows
  @BeforeEach
  @Override
  public void setUp() {
    setupMockCloudCredentialVendor();
    super.setUp();
    CredentialsApi adminCredentialsApi = new CredentialsApi(TestUtils.createApiClient(adminConfig));
    adminTempCredsApi = new TemporaryCredentialsApi(TestUtils.createApiClient(adminConfig));

    // Create a credential
    CreateCredentialRequest createCredentialRequest =
        new CreateCredentialRequest()
            .name(TEST_CREDENTIAL_NAME)
            .comment("Test credential for temporary path credentials")
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN));
    CredentialInfo credentialInfo = adminCredentialsApi.createCredential(createCredentialRequest);
    assertThat(credentialInfo).isNotNull();

    // Create a user and grant it permission to create a location
    String locationOwnerEmail = "location_owner@example.com";
    ApiClient locationOwnerClientApi = createApiClientForNewUser(locationOwnerEmail, List.of());
    ExternalLocationsApi locationOwnerExternalLocationsApi =
        new ExternalLocationsApi(locationOwnerClientApi);
    locationOwnerTempCredsApi = new TemporaryCredentialsApi(locationOwnerClientApi);
    grantPermissions(
        locationOwnerEmail,
        SecurableType.METASTORE,
        METASTORE_NAME,
        Privileges.CREATE_EXTERNAL_LOCATION);
    grantPermissions(
        locationOwnerEmail,
        SecurableType.CREDENTIAL,
        TEST_CREDENTIAL_NAME,
        Privileges.CREATE_EXTERNAL_LOCATION);

    // Create external location as locationOwner (not admin)
    CreateExternalLocation createExternalLocation =
        new CreateExternalLocation()
            .name(TEST_EXTERNAL_LOCATION_NAME)
            .url(TEST_EXTERNAL_LOCATION_URL)
            .credentialName(TEST_CREDENTIAL_NAME)
            .comment("Test external location");
    ExternalLocationInfo externalLocationInfo =
        locationOwnerExternalLocationsApi.createExternalLocation(createExternalLocation);
    assertThat(externalLocationInfo).isNotNull();

    // Create a user and grant it permission to create external tables and volumes
    tableVolumeCreatorApiClient =
        createApiClientForNewUser(
            TABLE_VOLUME_OWNER_EMAIL,
            List.of(Privileges.CREATE_EXTERNAL_TABLE, Privileges.CREATE_EXTERNAL_VOLUME));
    grantPermissions(
        TABLE_VOLUME_OWNER_EMAIL, SecurableType.CATALOG, CATALOG_NAME, Privileges.USE_CATALOG);
    grantPermissions(
        TABLE_VOLUME_OWNER_EMAIL,
        SecurableType.SCHEMA,
        SCHEMA_FULL_NAME,
        Privileges.USE_SCHEMA,
        Privileges.CREATE_TABLE,
        Privileges.CREATE_VOLUME);
  }

  @SneakyThrows
  private ApiClient createApiClientForNewUser(String email, List<Privileges> privileges) {
    createTestUser(email);
    for (Privileges privilege : privileges) {
      grantPermissions(
          email, SecurableType.EXTERNAL_LOCATION, TEST_EXTERNAL_LOCATION_NAME, privilege);
    }
    ServerConfig config = createTestUserServerConfig(email);
    return TestUtils.createApiClient(config);
  }

  private TemporaryCredentialsApi createTempCredApiForNewUser(
      String email, List<Privileges> privileges) {
    return new TemporaryCredentialsApi(createApiClientForNewUser(email, privileges));
  }

  @Getter
  private static class TestCase {
    public static final Set<PathOperation> ALL_OPERATIONS = Set.of(PathOperation.values());
    public static final Set<PathOperation> READ_WRITE = Set.of(PATH_READ, PATH_READ_WRITE);
    public static final Set<PathOperation> READONLY = Set.of(PATH_READ);
    public static final Set<PathOperation> CREATE_EXTERNAL_TABLE = Set.of(PATH_CREATE_TABLE);

    private final TemporaryCredentialsApi api;
    private final Set<PathOperation> expectOperations;

    TestCase(TemporaryCredentialsApi api, Set<PathOperation> expectOperations) {
      this.api = api;
      this.expectOperations = expectOperations;
    }
  }

  @Test
  public void testTemporaryPathCredentialsAuthorization() throws Exception {
    // 1. Test permission against external location itself. No data securables exist yet.

    TemporaryCredentialsApi readOnlyTempCredsApi =
        createTempCredApiForNewUser(READONLY_EMAIL, List.of(Privileges.READ_FILES));
    ApiClient readWriteUserApiClient =
        createApiClientForNewUser(
            READWRITE_EMAIL, List.of(Privileges.READ_FILES, Privileges.WRITE_FILES));
    TemporaryCredentialsApi readWriteTempCredsApi =
        new TemporaryCredentialsApi(readWriteUserApiClient);
    TemporaryCredentialsApi createTableTempCredsApi =
        createTempCredApiForNewUser(CREATE_TABLE_EMAIL, List.of(Privileges.CREATE_EXTERNAL_TABLE));
    TemporaryCredentialsApi unauthorizedTempCredsApi =
        createTempCredApiForNewUser(UNAUTHORIZED_EMAIL, List.of());

    // For URLs under the external location, follow external location permission.

    List<String> matchingUrls =
        List.of(TEST_EXTERNAL_LOCATION_URL, TEST_EXTERNAL_LOCATION_URL + "/subdir/nested");
    testPathCredentials(
        matchingUrls,
        List.of(
            new TestCase(adminTempCredsApi, TestCase.ALL_OPERATIONS),
            new TestCase(locationOwnerTempCredsApi, TestCase.ALL_OPERATIONS),
            new TestCase(readWriteTempCredsApi, TestCase.READ_WRITE),
            new TestCase(readOnlyTempCredsApi, TestCase.READONLY),
            new TestCase(createTableTempCredsApi, TestCase.CREATE_EXTERNAL_TABLE),
            new TestCase(unauthorizedTempCredsApi, Set.of())));

    // For URLs outside the external location, only metastore owner can get credential

    List<String> nonMatchingUrls = List.of("s3://test-bucket0/different/path");
    testPathCredentials(
        nonMatchingUrls,
        List.of(
            new TestCase(adminTempCredsApi, TestCase.ALL_OPERATIONS),
            new TestCase(locationOwnerTempCredsApi, Set.of()),
            new TestCase(unauthorizedTempCredsApi, Set.of())));

    // 2. Test permission when a table exists under the path

    createExternalTable(TEST_EXTERNAL_LOCATION_URL + "/tables/test_table");

    // Before granting permission, no regular user can access the table, not even the location owner

    testPathCredentials(
        List.of(
            TEST_EXTERNAL_LOCATION_URL + "/tables",
            TEST_EXTERNAL_LOCATION_URL + "/tables/test_table",
            TEST_EXTERNAL_LOCATION_URL + "/tables/test_table/subdir"),
        List.of(
            new TestCase(locationOwnerTempCredsApi, Set.of()),
            new TestCase(readWriteTempCredsApi, Set.of()),
            new TestCase(readOnlyTempCredsApi, Set.of()),
            new TestCase(createTableTempCredsApi, Set.of()),
            new TestCase(unauthorizedTempCredsApi, Set.of())));

    // Grant table permissions
    grantPermissions(
        READONLY_EMAIL, SecurableType.TABLE, TestUtils.TABLE_FULL_NAME, Privileges.SELECT);
    grantPermissions(
        READWRITE_EMAIL,
        SecurableType.TABLE,
        TestUtils.TABLE_FULL_NAME,
        Privileges.SELECT,
        Privileges.MODIFY);
    for (String email : List.of(READONLY_EMAIL, READWRITE_EMAIL)) {
      grantPermissions(
          email, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
      grantPermissions(
          email, SecurableType.SCHEMA, TestUtils.SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    }

    testPathCredentials(
        List.of(
            TEST_EXTERNAL_LOCATION_URL + "/tables/test_table",
            TEST_EXTERNAL_LOCATION_URL + "/tables/test_table/subdir"),
        List.of(
            new TestCase(adminTempCredsApi, TestCase.READ_WRITE),
            new TestCase(locationOwnerTempCredsApi, Set.of()),
            new TestCase(readWriteTempCredsApi, TestCase.READ_WRITE),
            new TestCase(readOnlyTempCredsApi, TestCase.READONLY),
            new TestCase(createTableTempCredsApi, Set.of()),
            new TestCase(unauthorizedTempCredsApi, Set.of())));

    // 3. Test permission when a volume exists under the path owned by TABLE_VOLUME_OWNER_EMAIL
    createExternalVolume(TEST_EXTERNAL_LOCATION_URL + "/volumes/test_volume");
    TemporaryCredentialsApi volumeOwnerApi =
        new TemporaryCredentialsApi(tableVolumeCreatorApiClient);

    // Before granting permission, no regular user except owner can access the volume. The location
    // owner can not access either.

    testPathCredentials(
        List.of(TEST_EXTERNAL_LOCATION_URL + "/volumes"),
        List.of(new TestCase(readWriteTempCredsApi, Set.of())));
    testPathCredentials(
        List.of(
            TEST_EXTERNAL_LOCATION_URL + "/volumes/test_volume",
            TEST_EXTERNAL_LOCATION_URL + "/volumes/test_volume/subdir"),
        List.of(new TestCase(volumeOwnerApi, TestCase.READ_WRITE)));
    testPathCredentials(
        List.of(
            TEST_EXTERNAL_LOCATION_URL + "/volumes",
            TEST_EXTERNAL_LOCATION_URL + "/volumes/test_volume",
            TEST_EXTERNAL_LOCATION_URL + "/volumes/test_volume/subdir"),
        List.of(
            new TestCase(locationOwnerTempCredsApi, Set.of()),
            new TestCase(readOnlyTempCredsApi, Set.of()),
            new TestCase(createTableTempCredsApi, Set.of()),
            new TestCase(readWriteTempCredsApi, Set.of()),
            new TestCase(unauthorizedTempCredsApi, Set.of())));

    // Grant readonly volume permissions
    grantPermissions(
        READONLY_EMAIL, SecurableType.VOLUME, TestUtils.VOLUME_FULL_NAME, Privileges.READ_VOLUME);

    testPathCredentials(
        List.of(
            TEST_EXTERNAL_LOCATION_URL + "/volumes/test_volume",
            TEST_EXTERNAL_LOCATION_URL + "/volumes/test_volume/subdir"),
        List.of(
            new TestCase(adminTempCredsApi, TestCase.READ_WRITE),
            new TestCase(locationOwnerTempCredsApi, Set.of()),
            new TestCase(readWriteTempCredsApi, Set.of()),
            new TestCase(readOnlyTempCredsApi, TestCase.READONLY),
            new TestCase(createTableTempCredsApi, Set.of()),
            new TestCase(unauthorizedTempCredsApi, Set.of())));

    // TODO: Test permission when a managed storage, or a model version exists under the path,
    //  once they are supported.

    // Finally, even admin can not access the parent path anymore due to data securables exist
    // under it.
    testPathCredentials(
        List.of(TEST_EXTERNAL_LOCATION_URL), List.of(new TestCase(adminTempCredsApi, Set.of())));
  }

  private void testPathCredentials(List<String> urls, List<TestCase> testCases) {
    List<PathOperation> subTestCases = List.of(PATH_READ, PATH_READ_WRITE, PATH_CREATE_TABLE);
    for (String url : urls) {
      for (TestCase testCase : testCases) {
        for (PathOperation operation : subTestCases) {
          boolean expectSuccess = testCase.getExpectOperations().contains(operation);
          if (expectSuccess) {
            testPathCredentialsSuccess(testCase.api, url, operation);
          } else {
            testPermissionDenied(testCase.api, url, operation);
          }
        }
      }
    }
  }

  @SneakyThrows
  private void testPathCredentialsSuccess(
      TemporaryCredentialsApi api, String url, PathOperation operation) {
    GenerateTemporaryPathCredential request =
        new GenerateTemporaryPathCredential().url(url).operation(operation);
    TemporaryCredentials creds = api.generateTemporaryPathCredentials(request);
    assertThat(creds).isNotNull();
    assertValidTemporaryCredentials(creds);
  }

  private void testPermissionDenied(
      TemporaryCredentialsApi api, String url, PathOperation operation) {
    GenerateTemporaryPathCredential request =
        new GenerateTemporaryPathCredential().url(url).operation(operation);
    assertApiException(
        () -> api.generateTemporaryPathCredentials(request),
        ErrorCode.PERMISSION_DENIED,
        "PERMISSION_DENIED");
  }

  private void assertValidTemporaryCredentials(TemporaryCredentials credentials) {
    assertThat(credentials).isNotNull();
    assertThat(credentials.getAwsTempCredentials()).isNotNull();

    AwsCredentials awsCreds = credentials.getAwsTempCredentials();
    assertThat(awsCreds.getAccessKeyId()).isNotNull().isNotEmpty();
    assertThat(awsCreds.getSecretAccessKey()).isNotNull().isNotEmpty();
    assertThat(awsCreds.getSessionToken()).isNotNull().isNotEmpty();

    // Expiration time should be set
    assertThat(credentials.getExpirationTime()).isNotNull();
  }

  @SneakyThrows
  private void createExternalTable(String storageLocation) {
    CreateTable createTableRequest =
        new CreateTable()
            .name(TestUtils.TABLE_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(
                List.of(
                    new ColumnInfo()
                        .name("id")
                        .typeText("INTEGER")
                        .typeJson("{\"type\": \"integer\"}")
                        .typeName(ColumnTypeName.INT)
                        .position(0)
                        .nullable(false)))
            .tableType(TableType.EXTERNAL)
            .storageLocation(storageLocation)
            .dataSourceFormat(DataSourceFormat.DELTA);
    TableInfo tableInfo =
        new TablesApi(tableVolumeCreatorApiClient).createTable(createTableRequest);
    assertThat(tableInfo).isNotNull();
  }

  @SneakyThrows
  private void createExternalVolume(String storageLocation) {
    CreateVolumeRequestContent createVolumeRequest =
        new CreateVolumeRequestContent()
            .name(TestUtils.VOLUME_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .volumeType(VolumeType.EXTERNAL)
            .storageLocation(storageLocation);
    VolumeInfo volumeInfo =
        new VolumesApi(tableVolumeCreatorApiClient).createVolume(createVolumeRequest);
    assertThat(volumeInfo).isNotNull();
  }

  public static class TestAwsCredentialsGenerator implements CredentialsGenerator {
    @Override
    public Credentials generate(CredentialContext ctx) {
      return Credentials.builder()
          .accessKeyId("test-access-key-id")
          .secretAccessKey("test-secret-access-key")
          .sessionToken("test-session-token")
          .expiration(Instant.now().plusSeconds(3600))
          .build();
    }
  }
}
