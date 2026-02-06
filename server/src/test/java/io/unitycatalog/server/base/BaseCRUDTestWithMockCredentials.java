package io.unitycatalog.server.base;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.AwsIamRoleResponse;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.TestingCredentialGenerator;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import io.unitycatalog.server.utils.UriScheme;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

public abstract class BaseCRUDTestWithMockCredentials extends BaseCRUDTest {
  @Mock AwsCredentialVendor awsCredentialVendor;
  @Mock AzureCredentialVendor azureCredentialVendor;
  @Mock GcpCredentialVendor gcpCredentialVendor;
  protected SchemaOperations schemaOperations;
  protected ExternalLocationsApi externalLocationsApi;
  protected CredentialsApi credentialsApi;

  protected static final String AWS_CREDENTIAL_NAME = "aws_credential";
  protected static final String AWS_EXTERNAL_LOCATION_NAME = "aws_external_location";
  protected static final String AWS_EXTERNAL_LOCATION_PARENT_PATH = "s3://external-location/test";
  protected static final String AWS_EXTERNAL_LOCATION_PATH = "s3://external-location/test/path";
  protected static final String TEST_AWS_STORAGE_CREDENTIAL_ROLE_ARN =
      "arn:aws:iam::987654321:role/UCDbRole-EXAMPLE";
  protected CredentialInfo awsCredentialInfo;

  @SneakyThrows
  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    ApiClient apiClient = TestUtils.createApiClient(serverConfig);
    externalLocationsApi = new ExternalLocationsApi(apiClient);
    credentialsApi = new CredentialsApi(apiClient);
    schemaOperations = createSchemaOperations(serverConfig);

    // Setup AWS external location + credential
    awsCredentialInfo = credentialsApi.createCredential(
        new CreateCredentialRequest()
            .name(AWS_CREDENTIAL_NAME)
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn(TEST_AWS_STORAGE_CREDENTIAL_ROLE_ARN)));
    externalLocationsApi.createExternalLocation(
        new CreateExternalLocation()
            .name(AWS_EXTERNAL_LOCATION_NAME)
            .url(AWS_EXTERNAL_LOCATION_PATH)
            .credentialName(AWS_CREDENTIAL_NAME));
    createCatalogAndSchema();
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.put("s3.bucketPath.0", "s3://test-bucket0");
    serverProperties.put("s3.accessKey.0", "accessKey0");
    serverProperties.put("s3.secretKey.0", "secretKey0");
    serverProperties.put("s3.sessionToken.0", "sessionToken0");

    // AWS S3 master role config
    serverProperties.put(
        ServerProperties.Property.AWS_MASTER_ROLE_ARN.getKey(),
        TestUtils.TEST_AWS_MASTER_ROLE_ARN);
    serverProperties.put(
        ServerProperties.Property.AWS_ACCESS_KEY.getKey(),
        TestUtils.TEST_AWS_MASTER_ROLE_ACCESS_KEY);
    serverProperties.put(
        ServerProperties.Property.AWS_SECRET_KEY.getKey(),
        TestUtils.TEST_AWS_MASTER_ROLE_SECRET_KEY);
    serverProperties.put(ServerProperties.Property.AWS_REGION.getKey(), TestUtils.TEST_AWS_REGION);

    serverProperties.put("gcs.bucketPath.0", "gs://test-bucket0");
    serverProperties.put("gcs.jsonKeyFilePath.0", "testing://0");
    serverProperties.put("gcs.credentialGenerator.0", TestingCredentialGenerator.class.getName());

    serverProperties.put("adls.storageAccountName.0", "test-bucket0");
    serverProperties.put("adls.tenantId.0", "tenantId0");
    serverProperties.put("adls.clientId.0", "clientId0");
    serverProperties.put("adls.clientSecret.0", "clientSecret0");
  }

  @Override
  protected void setUpCredentialOperations(ServerProperties serverProperties) {
    setupAwsCredentials(serverProperties);
    setupAzureCredentials();
    setupGcpCredentials();

    cloudCredentialVendor = new CloudCredentialVendor(
            awsCredentialVendor,
            azureCredentialVendor,
            gcpCredentialVendor
    );
  }

  @SneakyThrows
  private void createCatalogAndSchema() {
    CreateCatalog createCatalog =
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT);
    catalogOperations.createCatalog(createCatalog);

    schemaOperations.createSchema(
        new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
  }

  protected abstract SchemaOperations createSchemaOperations(ServerConfig config);

  private void setupAwsCredentials(ServerProperties serverProperties) {
    // Mock StsClient.builder() to return a EchoAwsStsClient. See EchoAwsStsClient in this file for
    // more details.
    awsCredentialVendor =
        new AwsCredentialVendor(
            serverProperties,
            () -> {
              StsClientBuilder mockBuilder = Mockito.mock(StsClientBuilder.class);
              when(mockBuilder.region(any())).thenReturn(mockBuilder);
              when(mockBuilder.credentialsProvider(any())).thenReturn(mockBuilder);
              when(mockBuilder.build()).thenReturn(new EchoAwsStsClient());
              return mockBuilder;
            });
  }

  private void setupAzureCredentials() {
    azureCredentialVendor = mock(AzureCredentialVendor.class);
    AzureCredential azureCredential = AzureCredential.builder()
            .sasToken("test-sas-token")
            .expirationTimeInEpochMillis(System.currentTimeMillis() + 6000)
            .build();
    serverProperties.entrySet().stream()
            .filter(e -> e.getKey().toString().startsWith("adls.storageAccountName"))
            .map(e -> e.getValue().toString())
            .forEach(path -> doReturn(azureCredential)
                    .when(azureCredentialVendor)
                    .vendAzureCredential(
                            argThat(isCredentialContextForCloudPath(UriScheme.ABFS, path))));
  }

  private void setupGcpCredentials() {
    gcpCredentialVendor = mock(GcpCredentialVendor.class);
    AccessToken gcpCredential = new AccessToken(
            "test-token",
            Date.from(Instant.now().plusSeconds(10 * 60))
    );
    serverProperties.entrySet().stream()
            .filter(e -> e.getKey().toString().startsWith("gcs.bucketPath"))
            .map(e -> e.getValue().toString())
            .forEach(path -> doReturn(gcpCredential)
                    .when(gcpCredentialVendor)
                    .vendGcpCredential(
                            argThat(isCredentialContextForCloudPath(UriScheme.GS, path))));
  }

  private ArgumentMatcher<CredentialContext> isCredentialContextForCloudPath(
      UriScheme scheme, String path) {
    return arg -> arg.getStorageScheme().equals(scheme)
        && arg.getStorageBase().toString().contains(path);
  }

  /**
   * @param scheme s3, abfs, gs
   * @param isConfiguredPath true if the path is configured in the server properties
   * @return Cloud path for testing
   */
  protected String getTestCloudPath(String scheme, boolean isConfiguredPath) {
    // test-bucket0 is configured in the properties
    String bucket = isConfiguredPath ? "test-bucket0" : "test-bucket1";
    return switch (scheme) {
      case "s3" -> "s3://" + bucket + "/test";
      case "abfs", "abfss" -> "abfs://test-container@" + bucket + ".dfs.core.windows.net/test";
      case "gs" -> "gs://" + bucket + "/test";
      default -> throw new IllegalArgumentException("Invalid scheme");
    };
  }

  protected void assertTemporaryCredentials(TemporaryCredentials tempCredentials, String scheme) {
    switch (scheme) {
      case "s3":
        AwsCredentials awsCredentials = tempCredentials.getAwsTempCredentials();
        assertThat(awsCredentials).isNotNull();
        assertThat(awsCredentials.getSessionToken()).isEqualTo("sessionToken0");
        assertThat(awsCredentials.getAccessKeyId()).isEqualTo("accessKey0");
        assertThat(awsCredentials.getSecretAccessKey()).isEqualTo("secretKey0");
        break;
      case "abfs":
      case "abfss":
        AzureUserDelegationSAS azureUserDelegationSAS = tempCredentials.getAzureUserDelegationSas();
        assertThat(azureUserDelegationSAS).isNotNull();
        assertThat(azureUserDelegationSAS.getSasToken()).isEqualTo("test-sas-token");
        break;
      case "gs":
        GcpOauthToken gcpOauthToken = tempCredentials.getGcpOauthToken();
        assertThat(gcpOauthToken).isNotNull();
        assertThat(gcpOauthToken.getOauthToken()).isEqualTo("test-token");
        break;
      default:
        fail("Invalid scheme");
        break;
    }
  }

  /**
   * isConfiguredPath is true if the path is configured in the server properties
   *
   * @return Stream of arguments (s3, abfs, gs) x isConfiguredPath (true, false) for testing
   */
  protected static Stream<Arguments> getArgumentsForParameterizedTests() {
    List<String> clouds = List.of("s3", "abfs", "gs");
    List<Boolean> isConfiguredPathFlags = List.of(true, false);

    // Cartesian product of clouds and isConfiguredPathFlags
    return clouds.stream()
            .flatMap(cloud -> isConfiguredPathFlags.stream()
                    .map(isConfiguredPath -> Arguments.of(cloud, isConfiguredPath)));
  }

  /**
   * A mock STS client for testing that examines input parameters and echos back some of them back
   * in the response. It can be useful to verify that the right ARN and external is being used, and
   * test can further examine the content of session policy by checking its response.
   * This directly replaces and mocks the real AWS so that in all the tests based on this file
   * the entire AWS credential vending logic is exercised to the point of calling
   * `stsClient.assumeRole()`.
   */
  protected class EchoAwsStsClient implements StsClient {
    public static final String RETURN_ACCESS_KEY = "assumedRoleAccessKey";
    public static final String RETURN_SECRET_KEY = "assumedRoleSecretKey";

    public EchoAwsStsClient() {}

    @Override
    public AssumeRoleResponse assumeRole(AssumeRoleRequest request) {
      AwsIamRoleResponse awsIamRole = awsCredentialInfo.getAwsIamRole();
      assertThat(request.roleArn()).isEqualTo(awsIamRole.getRoleArn());
      assertThat(request.externalId()).isEqualTo(awsIamRole.getExternalId());
      String echoSessionToken =
          generateSessionToken(request.roleSessionName(), request.policy());
      return AssumeRoleResponse.builder()
          .credentials(
              Credentials.builder()
                  .accessKeyId(RETURN_ACCESS_KEY)
                  .secretAccessKey(RETURN_SECRET_KEY)
                  .sessionToken(echoSessionToken)
                  .expiration(Instant.now().plusSeconds(10 * 60))
                  .build())
          .build();
    }

    public static String generateSessionToken(String sessionName, String policy) {
      return String.format("%s@%s", sessionName, policy);
    }

    public static void assertAwsCredential(TemporaryCredentials tempCredentials) {
      assertThat(tempCredentials.getExpirationTime()).isGreaterThan(System.currentTimeMillis());
      assertThat(tempCredentials.getAwsTempCredentials()).isNotNull();
      AwsCredentials getAwsTempCredentials = tempCredentials.getAwsTempCredentials();
      assertThat(getAwsTempCredentials.getAccessKeyId()).isEqualTo(RETURN_ACCESS_KEY);
      assertThat(getAwsTempCredentials.getSecretAccessKey()).isEqualTo(RETURN_SECRET_KEY);
    }

    @Override
    public String serviceName() {
      return "";
    }

    @Override
    public void close() {}
  }
}
