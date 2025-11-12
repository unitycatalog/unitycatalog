package io.unitycatalog.server.service;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.auth0.jwt.JWT;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.auth.AuthToken;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.access.BaseAccessControlCRUDTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.model.CreateUser;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.security.JwtTokenType;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Date;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TemporaryPathCredentialsServiceTest extends BaseAccessControlCRUDTest {
  private static final String ENDPOINT = "/api/2.1/unity-catalog/temporary-path-credentials";
  private static final String TEST_CATALOG = "test_catalog";
  private static final String TEST_SCHEMA = "test_schema";
  private static final String ACCESS_DENIED_CATALOG = "access_denied_catalog";
  private static final String ACCESS_DENIED_SCHEMA = "access_denied_schema";

  private static final String ADMIN_USER = "admin";
  private static final String SCHEMA_USER = "schema_user@test.com";
  private static final String CATALOG_USER = "catalog_user@test.com";

  private WebClient client;
  private SchemaOperations schemaOperations;
  private CatalogOperations catalogOperations;

  private static RequestHeaders createHeaders() {
    return RequestHeaders.builder()
        .method(HttpMethod.POST)
        .path(ENDPOINT)
        .contentType(MediaType.JSON)
        .build();
  }

  private static Stream<Arguments> createPermissionsTestCases() {
    return Stream.of(
        // Table path - admin user has OWNER privilege, should succeed for all operations
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG + "/" + TEST_SCHEMA + "/table",
            ADMIN_USER,
            HttpStatus.OK,
            PathOperation.PATH_READ),
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG + "/" + TEST_SCHEMA + "/table",
            ADMIN_USER,
            HttpStatus.OK,
            PathOperation.PATH_READ_WRITE),
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG + "/" + TEST_SCHEMA + "/table",
            ADMIN_USER,
            HttpStatus.OK,
            PathOperation.PATH_CREATE_TABLE),
        Arguments.of(
            "s3://bucket/" + ACCESS_DENIED_CATALOG + "/" + ACCESS_DENIED_SCHEMA + "/table",
            ADMIN_USER,
            HttpStatus.OK,
            PathOperation.PATH_CREATE_TABLE),

        // Schema path - schema user has USE_SCHEMA privilege, should succeed for CREATE_TABLE
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG + "/" + TEST_SCHEMA + "/table",
            SCHEMA_USER,
            HttpStatus.OK,
            PathOperation.PATH_CREATE_TABLE),
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG + "/" + TEST_SCHEMA + "/",
            SCHEMA_USER,
            HttpStatus.OK,
            PathOperation.PATH_CREATE_TABLE),

        // Schema path - schema user is not OWNER, should fail for READ/READ_WRITE
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG + "/" + TEST_SCHEMA + "/",
            SCHEMA_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_READ),
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG + "/" + TEST_SCHEMA + "/",
            SCHEMA_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_READ_WRITE),

        // Schema path - schema user lacks USE_SCHEMA privilege on ACCESS_DENIED_SCHEMA, should fail
        // for CREATE_TABLE
        Arguments.of(
            "s3://bucket/" + ACCESS_DENIED_CATALOG + "/" + ACCESS_DENIED_SCHEMA + "/table",
            SCHEMA_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_CREATE_TABLE),

        // Schema path - catalog user lacks USE_SCHEMA privilege, should fail for CREATE_TABLE
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG + "/" + TEST_SCHEMA + "/table",
            CATALOG_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_CREATE_TABLE),

        // Catalog path - catalog user has USE_CATALOG privilege, should succeed for CREATE_TABLE
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG,
            CATALOG_USER,
            HttpStatus.OK,
            PathOperation.PATH_CREATE_TABLE),
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG,
            CATALOG_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_READ),
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG,
            CATALOG_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_READ_WRITE),

        // Catalog path - catalog user lacks USE_SCHEMA privilege on ACCESS_DENIED_SCHEMA, should
        // fail for CREATE_TABLE
        Arguments.of(
            "s3://bucket/" + ACCESS_DENIED_CATALOG + "/" + ACCESS_DENIED_SCHEMA + "/table",
            CATALOG_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_CREATE_TABLE),
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG + "/" + ACCESS_DENIED_SCHEMA + "/table",
            CATALOG_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_CREATE_TABLE),

        // Schema path - schema user lacks USE_SCHEMA privilege, should fail for CREATE_TABLE
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG + "/" + ACCESS_DENIED_SCHEMA + "/table",
            SCHEMA_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_CREATE_TABLE),

        // Catalog path - schema user has USE_CATALOG privilege, should succeed for CREATE_TABLE
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG,
            SCHEMA_USER,
            HttpStatus.OK,
            PathOperation.PATH_CREATE_TABLE),
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG,
            SCHEMA_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_READ),
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG,
            SCHEMA_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_READ_WRITE),

        // Base path (no catalog/schema) - only ADMIN should succeed
        Arguments.of("s3://bucket/", ADMIN_USER, HttpStatus.OK, PathOperation.PATH_CREATE_TABLE),
        Arguments.of(
            "s3://bucket/", SCHEMA_USER, HttpStatus.FORBIDDEN, PathOperation.PATH_CREATE_TABLE),
        Arguments.of(
            "s3://bucket/", CATALOG_USER, HttpStatus.FORBIDDEN, PathOperation.PATH_CREATE_TABLE),

        // Test all operations for ADMIN on ACCESS_DENIED_CATALOG (metastore owner access)
        Arguments.of(
            "s3://bucket/" + ACCESS_DENIED_CATALOG + "/" + ACCESS_DENIED_SCHEMA + "/table",
            ADMIN_USER,
            HttpStatus.OK,
            PathOperation.PATH_READ),
        Arguments.of(
            "s3://bucket/" + ACCESS_DENIED_CATALOG + "/" + ACCESS_DENIED_SCHEMA + "/table",
            ADMIN_USER,
            HttpStatus.OK,
            PathOperation.PATH_READ_WRITE),

        // SCHEMA_USER should fail on PATH_READ/PATH_READ_WRITE on their schema
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG + "/" + TEST_SCHEMA + "/table",
            SCHEMA_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_READ),
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG + "/" + TEST_SCHEMA + "/table",
            SCHEMA_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_READ_WRITE),

        // CATALOG_USER should fail on PATH_READ and PATH_READ_WRITE on catalog-level path
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG,
            CATALOG_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_READ),
        Arguments.of(
            "s3://bucket/" + TEST_CATALOG,
            CATALOG_USER,
            HttpStatus.FORBIDDEN,
            PathOperation.PATH_READ_WRITE),

        // Test with local path format
        Arguments.of(
            "/" + TEST_CATALOG + "/" + TEST_SCHEMA + "/table",
            SCHEMA_USER,
            HttpStatus.OK,
            PathOperation.PATH_CREATE_TABLE),

        Arguments.of(
            "file:/" + TEST_CATALOG + "/" + TEST_SCHEMA + "/table",
            SCHEMA_USER,
            HttpStatus.OK,
            PathOperation.PATH_CREATE_TABLE
        ));
  }

  @Override
  protected void setUpCredentialOperations() {
    // Mock the CloudCredentialVendor directly to return empty credentials
    cloudCredentialVendor = mock(CloudCredentialVendor.class);

    io.unitycatalog.server.model.TemporaryCredentials mockCreds =
        new io.unitycatalog.server.model.TemporaryCredentials();

    // Mock the vendCredential method that takes String and Set<Privilege>
    when(cloudCredentialVendor.vendCredential(anyString(), any())).thenReturn(mockCreds);
  }

  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  @BeforeEach
  @SneakyThrows
  public void setUp() {
    // Set admin token to initialize the test environment
    super.setUp();
    switchUser(ADMIN_USER);

    catalogOperations = createCatalogOperations(serverConfig);
    schemaOperations = createSchemaOperations(serverConfig);

    getOrCreateUser(SCHEMA_USER);
    getOrCreateUser(CATALOG_USER);

    catalogOperations.createCatalog(new CreateCatalog().name(ACCESS_DENIED_CATALOG));
    schemaOperations.createSchema(
        new CreateSchema().name(ACCESS_DENIED_SCHEMA).catalogName(ACCESS_DENIED_CATALOG));

    catalogOperations.createCatalog(new CreateCatalog().name(TEST_CATALOG));
    schemaOperations.createSchema(new CreateSchema().name(TEST_SCHEMA).catalogName(TEST_CATALOG));

    grantPermissionViaApi(
        SCHEMA_USER, TEST_CATALOG + "." + TEST_SCHEMA, "schema", Privileges.USE_SCHEMA);
    grantPermissionViaApi(SCHEMA_USER, TEST_CATALOG, "catalog", Privileges.USE_CATALOG);
    grantPermissionViaApi(CATALOG_USER, TEST_CATALOG, "catalog", Privileges.USE_CATALOG);

    // Create another schema under TEST_CATALOG that users don't have explicit access to
    schemaOperations.createSchema(
        new CreateSchema().name(ACCESS_DENIED_SCHEMA).catalogName(TEST_CATALOG));
  }

  @AfterEach
  public void cleanUp() {
    super.tearDown();
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
      String principal, String fullName, String resourceType, Privileges addedPrivilege) {
    String requestBody =
        String.format(
            "{\"changes\":[{\"principal\":\"%s\",\"add\":[\"%s\"]}]}",
            principal, addedPrivilege.getValue());
    RequestHeaders headers =
        RequestHeaders.builder()
            .method(HttpMethod.PATCH)
            .path(String.format("/api/2.1/unity-catalog/permissions/%s/%s", resourceType, fullName))
            .contentType(MediaType.JSON)
            .build();

    client.execute(headers, requestBody).aggregate().join();
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
