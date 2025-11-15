package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.EXTERNAL_LOCATION;
import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.auth0.jwt.JWT;
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
import io.unitycatalog.server.base.BaseCRUDTestWithMockCredentials;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.credential.CredentialOperations;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.model.CreateUser;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.sdk.externallocation.SdkExternalLocationOperations;
import io.unitycatalog.server.sdk.storagecredential.SdkCredentialOperations;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.security.JwtTokenType;
import io.unitycatalog.server.security.SecurityConfiguration;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.utils.TestUtils;
import java.nio.file.Path;
import java.util.Date;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TemporaryPathCredentialsServiceTest extends BaseCRUDTestWithMockCredentials {
  private static final String ENDPOINT = "/api/2.1/unity-catalog/temporary-path-credentials";
  private static final String ROOT_PATH = "s3://test-bucket0";
  private static final String CREDENTIAL_NAME = "test_credential";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";
  private static final String EXTERNAL_LOCATION_NAME = "test_ext_location";
  private static final String EXTERNAL_LOCATION_URL = ROOT_PATH + "/external";

  private static final String ADMIN_USER = "admin";
  private static final String AUTHORIZED_USER = "authorized@test.com";
  private static final String DENIED_USER = "denied@test.com";
  protected SecurityConfiguration securityConfiguration;
  protected SecurityContext securityContext;
  private WebClient client;
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

  @BeforeEach
  @SneakyThrows
  public void setUp() {
    // Set admin token to initialize the test environment
    super.setUp();
    Path configurationFolder = Path.of("etc", "conf");

    securityConfiguration = new SecurityConfiguration(configurationFolder);
    securityContext =
        new SecurityContext(configurationFolder, securityConfiguration, "server", INTERNAL);

    switchUser(ADMIN_USER);

    credentialOperations = createCredentialOperations(serverConfig);
    externalLocationOperations = createExternalLocationOperations(serverConfig);

    getOrCreateUser(AUTHORIZED_USER);
    getOrCreateUser(DENIED_USER);

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

    grantPermissionViaApi(
        AUTHORIZED_USER,
        EXTERNAL_LOCATION_NAME,
        EXTERNAL_LOCATION,
        Privileges.EXTERNAL_USE_LOCATION);
  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return null;
  }

  @AfterEach
  public void cleanUp() {
    System.clearProperty("server.authorization");

    SessionFactory sessionFactory = hibernateConfigurator.getSessionFactory();
    Session session = sessionFactory.openSession();
    Transaction tx = session.beginTransaction();
    session.createNativeMutationQuery("delete from casbin_rule").executeUpdate();
    tx.commit();
    session.close();

    super.tearDown();
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty("server.authorization", "enable");
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
    switchUser(userEmail);

    // Act
    AggregatedHttpResponse response =
        client.execute(createHeaders(), requestBody).aggregate().join();

    // Assert
    assertThat(response.status()).isEqualTo(expectedStatus);
  }

  @SneakyThrows
  private String createUserIfNotExists(String email) {
    getOrCreateUser(email);

    return createToken(email);
  }

  private void grantPermissionViaApi(
      String principal, String fullName, SecurableType resourceType, Privileges addedPrivilege) {
    String requestBody =
        String.format(
            "{\"changes\":[{\"principal\":\"%s\",\"add\":[\"%s\"]}]}",
            principal, addedPrivilege.getValue());
    RequestHeaders headers =
        RequestHeaders.builder()
            .method(HttpMethod.PATCH)
            .path(
                String.format(
                    "/api/2.1/unity-catalog/permissions/%s/%s", resourceType.getValue(), fullName))
            .contentType(MediaType.JSON)
            .build();

    AggregatedHttpResponse response = client.execute(headers, requestBody).aggregate().join();
    if (response.status() != HttpStatus.OK) {
      throw new IllegalStateException("Error setting up permission: " + response.contentUtf8());
    }
  }

  private void getOrCreateUser(String email) {
    Repositories repositories =
        new Repositories(
            hibernateConfigurator.getSessionFactory(),
            new io.unitycatalog.server.utils.ServerProperties(serverProperties));

    try {
      repositories.getUserRepository().getUserByEmail(email);
    } catch (BaseException e) {
      CreateUser createUser =
          CreateUser.builder().active(true).email(email).name(email.split("@")[0]).build();

      repositories.getUserRepository().createUser(createUser);
    }
  }

  @SneakyThrows
  private String createToken(String email) {
    if (email.equals(ADMIN_USER)) {
      return securityContext.getServiceToken();
    }
    return JWT.create()
        .withSubject(securityContext.getServiceName())
        .withIssuer(securityContext.getLocalIssuer())
        .withIssuedAt(new Date())
        .withKeyId(securityConfiguration.getKeyId())
        .withJWTId(UUID.randomUUID().toString())
        .withClaim(JwtClaim.TOKEN_TYPE.key(), JwtTokenType.ACCESS.name())
        .withClaim(JwtClaim.SUBJECT.key(), email)
        .sign(securityConfiguration.algorithmRSA());
  }

  private void switchUser(String email) {
    String uri = serverConfig.getServerUrl();
    String token = createUserIfNotExists(email);
    serverConfig.setAuthToken(token);
    client = WebClient.builder(uri).auth(AuthToken.ofOAuth2(token)).build();
  }
}
