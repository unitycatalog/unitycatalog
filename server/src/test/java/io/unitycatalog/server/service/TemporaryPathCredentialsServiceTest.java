package io.unitycatalog.server.service;

import static io.unitycatalog.client.model.SecurableType.EXTERNAL_LOCATION;
import static io.unitycatalog.server.persist.model.Privileges.EXTERNAL_USE_LOCATION;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.auth.AuthToken;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.credential.CredentialOperations;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.sdk.access.SdkAccessControlBaseCRUDTest;
import io.unitycatalog.server.sdk.externallocation.SdkExternalLocationOperations;
import io.unitycatalog.server.sdk.storagecredential.SdkCredentialOperations;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.utils.TestUtils;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import software.amazon.awssdk.services.sts.model.Credentials;

public class TemporaryPathCredentialsServiceTest extends SdkAccessControlBaseCRUDTest {
  private static final String ENDPOINT = "/api/2.1/unity-catalog/temporary-path-credentials";
  private static final String ROOT_PATH = "s3://test-bucket0";
  private static final String CREDENTIAL_NAME = "test_credential";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";
  private static final String EXTERNAL_LOCATION_NAME = "test_ext_location";
  private static final String EXTERNAL_LOCATION_URL = ROOT_PATH + "/external";

  private static final String ADMIN_USER = "admin";
  private static final String AUTHORIZED_USER = "authorized@test.com";
  private static final String DENIED_USER = "denied@test.com";
  @Mock AwsCredentialVendor awsCredentialVendor;
  private ExternalLocationOperations externalLocationOperations;
  private CredentialOperations credentialOperations;

  private static Stream<Arguments> createPermissionsTestCases() {
    return Stream.of(
        // metastore OWNER can have access to the external location because he is the owner of it
        Arguments.of(
            EXTERNAL_LOCATION_URL + "/table",
            ADMIN_USER,
            HttpStatus.OK,
            PathOperation.PATH_CREATE_TABLE),
        Arguments.of(
            EXTERNAL_LOCATION_URL + "/table", ADMIN_USER, HttpStatus.OK, PathOperation.PATH_READ),
        Arguments.of(
            EXTERNAL_LOCATION_URL + "/table",
            ADMIN_USER,
            HttpStatus.OK,
            PathOperation.PATH_READ_WRITE),

        // user with EXTERNAL_USE_LOCATION privilege can access the external location
        Arguments.of(
            EXTERNAL_LOCATION_URL + "/table",
            AUTHORIZED_USER,
            HttpStatus.OK,
            PathOperation.PATH_CREATE_TABLE),
        Arguments.of(
            EXTERNAL_LOCATION_URL + "/table",
            AUTHORIZED_USER,
            HttpStatus.OK,
            PathOperation.PATH_READ),
        Arguments.of(
            EXTERNAL_LOCATION_URL + "/table",
            AUTHORIZED_USER,
            HttpStatus.OK,
            PathOperation.PATH_READ_WRITE),

        // user without EXTERNAL_USE_LOCATION privilege cannot access the external location
        Arguments.of(
            EXTERNAL_LOCATION_URL + "/table",
            DENIED_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_CREATE_TABLE),
        Arguments.of(
            EXTERNAL_LOCATION_URL + "/table",
            DENIED_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_READ),
        Arguments.of(
            EXTERNAL_LOCATION_URL + "/table",
            DENIED_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_READ_WRITE));
  }

  private static RequestHeaders createHeaders() {
    return RequestHeaders.builder()
        .method(HttpMethod.POST)
        .path(ENDPOINT)
        .contentType(MediaType.JSON)
        .build();
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.put("s3.bucketPath.0", "s3://test-bucket0");
    serverProperties.put("s3.accessKey.0", "accessKey0");
    serverProperties.put("s3.secretKey.0", "secretKey0");
    serverProperties.put("s3.sessionToken.0", "sessionToken0");
  }

  @Override
  protected void setUpCredentialOperations() {
    setupAwsCredentials();

    cloudCredentialVendor = new CloudCredentialVendor(awsCredentialVendor, null, null);
  }

  private void setupAwsCredentials() {
    awsCredentialVendor = mock(AwsCredentialVendor.class);
    Credentials awsCredential =
        Credentials.builder()
            .accessKeyId("test-access-key-id")
            .secretAccessKey("test-secret-access-key")
            .sessionToken("test-session-token")
            .build();
    serverProperties.entrySet().stream()
        .filter(e -> e.getKey().toString().startsWith("s3.bucketPath"))
        .map(e -> e.getValue().toString())
        .forEach(
            path ->
                doReturn(awsCredential)
                    .when(awsCredentialVendor)
                    .vendAwsCredentials(argThat(isCredentialContextForCloudPath("s3", path))));
  }

  private ArgumentMatcher<CredentialContext> isCredentialContextForCloudPath(
      String scheme, String path) {
    return arg -> arg.getStorageScheme().equals(scheme) && arg.getStorageBase().contains(path);
  }

  @SneakyThrows
  @BeforeEach
  public void setUp() {
    super.setUp();

    credentialOperations = createCredentialOperations(adminConfig);
    externalLocationOperations = createExternalLocationOperations(adminConfig);

    credentialOperations.createCredential(
        new CreateCredentialRequest()
            .name(CREDENTIAL_NAME)
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN)));
    externalLocationOperations.createExternalLocation(
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME)
            .url(EXTERNAL_LOCATION_URL)
            .credentialName(CREDENTIAL_NAME));

    createTestUser(AUTHORIZED_USER, AUTHORIZED_USER.split("@")[0]);
    createTestUser(DENIED_USER, DENIED_USER.split("@")[0]);
    grantPermissions(
        AUTHORIZED_USER, EXTERNAL_LOCATION, EXTERNAL_LOCATION_NAME, EXTERNAL_USE_LOCATION);
  }

  protected CredentialOperations createCredentialOperations(ServerConfig serverConfig) {
    return new SdkCredentialOperations(TestUtils.createApiClient(serverConfig));
  }

  protected ExternalLocationOperations createExternalLocationOperations(ServerConfig serverConfig) {
    return new SdkExternalLocationOperations(TestUtils.createApiClient(serverConfig));
  }

  @ParameterizedTest
  @MethodSource("createPermissionsTestCases")
  public void testGenerateTemporaryPathCredential(
      String url, String userEmail, HttpStatus expectedStatus, PathOperation operation) {
    // Arrange
    String requestBody =
        String.format("{\"url\":\"%s\",\"operation\":\"%s\"}", url, operation.toString());

    String uri = serverConfig.getServerUrl();
    WebClient webClient =
        WebClient.builder(uri).auth(AuthToken.ofOAuth2(createTestJwtToken(userEmail))).build();

    //    // Act
    AggregatedHttpResponse response =
        webClient.execute(createHeaders(), requestBody).aggregate().join();

    // Assert
    assertThat(response.status()).isEqualTo(expectedStatus);
  }
}
