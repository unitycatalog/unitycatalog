/*
 * Copyright 2026 The Unity Catalog Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.unitycatalog.server.auth;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;
import static io.unitycatalog.server.service.credential.CredentialContext.READ_ONLY;
import static io.unitycatalog.server.service.credential.CredentialContext.READ_WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.auth0.jwt.JWT;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.auth.AuthToken;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.model.CreateUser;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.security.SecurityConfiguration;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.LocalMappingFileOperations;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

/** Exercises real UC HTTP requests against a real OPA policy; only cloud access is mocked. */
@Testcontainers(disabledWithoutDocker = true)
public class OpaAuthorizationTest extends BaseServerTest {
  private static final String UC = "/api/2.1/unity-catalog";
  private static final String ICEBERG =
      UC + "/iceberg/v1/catalogs/visible/namespaces/schema/tables";
  private static final String CLOUD_ROOT = "s3://test-bucket/data";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Container
  private static final GenericContainer<?> OPA =
      new GenericContainer<>("openpolicyagent/opa:1.21.4")
          .withExposedPorts(8181)
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("opa/authorization.rego"), "/authorization.rego")
          .withCommand("run", "--server", "--addr=0.0.0.0:8181", "/authorization.rego")
          .waitingFor(Wait.forHttp("/health").forStatusCode(200));

  private Repositories repositories;
  private SecurityConfiguration securityConfiguration;
  private LocalMappingFileOperations mappingFileOperations;

  private static String opaUrl() {
    return "http://" + OPA.getHost() + ":" + OPA.getMappedPort(8181);
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.AUTHORIZATION_ENABLED.getKey(), "enable");
    serverProperties.setProperty(Property.AUTHORIZATION_PROVIDER.getKey(), "opa");
    serverProperties.setProperty(
        Property.OPA_POLICY_URL.getKey(), opaUrl() + "/v1/data/unitycatalog/decision");
    serverProperties.setProperty(Property.MODEL_STORAGE_ROOT.getKey(), CLOUD_ROOT + "/models");
  }

  @Override
  protected void setUpCredentialOperations(ServerProperties properties) {
    cloudCredentialVendor = mock(CloudCredentialVendor.class);
    when(cloudCredentialVendor.vendCredential(any()))
        .thenReturn(
            new TemporaryCredentials()
                .awsTempCredentials(
                    new AwsCredentials()
                        .accessKeyId("test-access-key")
                        .secretAccessKey("test-secret")
                        .sessionToken("test-session"))
                .expirationTime(System.currentTimeMillis() + 60_000));
  }

  @Override
  protected FileOperations decorateFileOperations(FileOperations fileOperations) {
    mappingFileOperations =
        new LocalMappingFileOperations(
            fileOperations, Map.of(S3FileIOProperties.ACCESS_KEY_ID, "test-access-key"));
    mappingFileOperations.mapLocation(
        NormalizedURL.from(CLOUD_ROOT), testDirectoryRoot.resolve("cloud"));
    return mappingFileOperations;
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    repositories =
        new Repositories(
            hibernateConfigurator.getSessionFactory(),
            new ServerProperties(serverProperties),
            cloudCredentialVendor);
    securityConfiguration = new SecurityConfiguration(Path.of("etc", "conf"));
    for (String name : List.of("reader", "writer", "write_only", "invalid")) {
      repositories
          .getUserRepository()
          .createUser(CreateUser.builder().name(name).email(name + "@example.com").build());
    }
    setPolicyData(Map.of());
  }

  @AfterEach
  @Override
  public void tearDown() {
    if (unityCatalogServer != null) {
      try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
        Transaction transaction = session.beginTransaction();
        session.createNativeMutationQuery("delete from casbin_rule").executeUpdate();
        transaction.commit();
      }
    }
    super.tearDown();
  }

  @Test
  public void opaAllowsWithoutUcGrantsAndCanDenyTheNativeOwner() {
    JsonNode created =
        json(request("writer", HttpMethod.POST, UC + "/catalogs", Map.of("name", "visible")), 200);
    assertThat(created.path("owner").asText()).isEqualTo("writer@example.com");
    assertThat(repositories.getCatalogRepository().getCatalog("visible").getId())
        .isEqualTo(created.path("id").asText());

    assertThat(request("writer", HttpMethod.DELETE, UC + "/catalogs/visible", null).status())
        .isEqualTo(HttpStatus.FORBIDDEN);
    assertThat(repositories.getCatalogRepository().getCatalog("visible").getName())
        .isEqualTo("visible");

    assertThat(
            request("writer", HttpMethod.POST, UC + "/catalogs", Map.of("name", "denied")).status())
        .isEqualTo(HttpStatus.FORBIDDEN);
    assertThatThrownBy(() -> repositories.getCatalogRepository().getCatalog("denied"))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("Catalog not found");
  }

  @Test
  public void listsUseOpaForEachItemAndPolicyErrorsFailTheRequest() {
    createCatalogAndSchema();
    json(request("admin", HttpMethod.POST, UC + "/catalogs", Map.of("name", "hidden")), 200);

    JsonNode listed = json(request("reader", HttpMethod.GET, UC + "/catalogs", null), 200);
    assertThat(listed.path("catalogs").findValuesAsText("name")).containsExactly("visible");
    assertThat(request("reader", HttpMethod.GET, UC + "/catalogs/hidden", null).status())
        .isEqualTo(HttpStatus.FORBIDDEN);
    JsonNode error = json(request("invalid", HttpMethod.GET, UC + "/catalogs", null), 500);
    assertThat(error.path("message").asText()).isEqualTo("Authorization policy evaluation failed.");
  }

  @Test
  public void invalidPolicyDecisionsCannotMutateButAuthenticationOnlyEndpointsStillWork() {
    json(request("invalid", HttpMethod.POST, UC + "/catalogs", Map.of("name", "invalid")), 500);
    assertThatThrownBy(() -> repositories.getCatalogRepository().getCatalog("invalid"))
        .isInstanceOf(BaseException.class);
    assertThat(request("invalid", HttpMethod.GET, "/api/1.0/unity-control/scim2/Me", null).status())
        .isEqualTo(HttpStatus.OK);
    assertThat(
            WebClient.of(serverConfig.getServerUrl())
                .get(UC + "/catalogs")
                .aggregate()
                .join()
                .status())
        .isEqualTo(HttpStatus.UNAUTHORIZED);
  }

  @Test
  public void pathCredentialsUseTheResolvedOwnerAndKeepLocalOverlapChecks() {
    Map<String, String> ids = createStorageResources();
    JsonNode table = createExternalTable("table");
    ids.put("table", table.path("table_id").asText());
    JsonNode volume =
        json(
            request(
                "admin",
                HttpMethod.POST,
                UC + "/volumes",
                Map.of(
                    "name",
                    "volume",
                    "catalog_name",
                    "visible",
                    "schema_name",
                    "schema",
                    "volume_type",
                    "EXTERNAL",
                    "storage_location",
                    CLOUD_ROOT + "/volume")),
            200);
    ids.put("volume", volume.path("volume_id").asText());
    JsonNode model =
        json(
            request(
                "admin",
                HttpMethod.POST,
                UC + "/models",
                Map.of("name", "model", "catalog_name", "visible", "schema_name", "schema")),
            200);
    ids.put("registered_model", model.path("id").asText());
    setPolicyData(ids);

    for (String path :
        List.of(
            CLOUD_ROOT + "/free",
            CLOUD_ROOT + "/table/file",
            CLOUD_ROOT + "/volume/file",
            model.path("storage_location").asText() + "/file")) {
      clearInvocations(cloudCredentialVendor);
      json(pathCredentials("reader", path, "PATH_READ"), 200);
      ArgumentCaptor<CredentialContext> scope = ArgumentCaptor.forClass(CredentialContext.class);
      verify(cloudCredentialVendor).vendCredential(scope.capture());
      assertThat(scope.getValue().getPrivileges()).isEqualTo(READ_ONLY);
      assertThat(scope.getValue().getLocations()).containsExactly(NormalizedURL.from(path));
      clearInvocations(cloudCredentialVendor);
      assertThat(pathCredentials("reader", path, "PATH_READ_WRITE").status())
          .isEqualTo(HttpStatus.FORBIDDEN);
      verifyNoInteractions(cloudCredentialVendor);
    }

    clearInvocations(cloudCredentialVendor);
    assertThat(pathCredentials("admin", CLOUD_ROOT + "/table/file", "PATH_CREATE_TABLE").status())
        .isEqualTo(HttpStatus.FORBIDDEN);
    assertThat(pathCredentials("admin", CLOUD_ROOT + "/free", "UNKNOWN_PATH_OPERATION").status())
        .isEqualTo(HttpStatus.FORBIDDEN);
    assertThat(request("admin", HttpMethod.POST, UC + "/tables", externalTable("overlap")).status())
        .isEqualTo(HttpStatus.FORBIDDEN);
    verifyNoInteractions(cloudCredentialVendor);
  }

  @Test
  public void icebergCredentialsRequireReadAndOnlyWidenWhenOpaAllowsWrite() {
    Map<String, String> ids = createStorageResources();
    JsonNode created =
        json(
            request(
                "admin",
                HttpMethod.POST,
                ICEBERG,
                Map.of(
                    "name",
                    "iceberg",
                    "location",
                    CLOUD_ROOT + "/iceberg",
                    "schema",
                    Map.of(
                        "type",
                        "struct",
                        "schema-id",
                        0,
                        "fields",
                        List.of(Map.of("id", 1, "name", "id", "required", true, "type", "long"))))),
            200);
    assertThat(created.path("metadata").path("table-uuid").asText()).isNotBlank();
    ids.put(
        "table", repositories.getTableRepository().getTable("visible.schema.iceberg").getTableId());
    setPolicyData(ids);

    for (String caller : List.of("reader", "writer")) {
      clearInvocations(cloudCredentialVendor);
      json(request(caller, HttpMethod.GET, ICEBERG + "/iceberg/credentials", null), 200);
      ArgumentCaptor<CredentialContext> scope = ArgumentCaptor.forClass(CredentialContext.class);
      verify(cloudCredentialVendor).vendCredential(scope.capture());
      assertThat(scope.getValue().getPrivileges())
          .isEqualTo(caller.equals("writer") ? READ_WRITE : READ_ONLY);
    }
    clearInvocations(cloudCredentialVendor);
    assertThat(
            request("write_only", HttpMethod.GET, ICEBERG + "/iceberg/credentials", null).status())
        .isEqualTo(HttpStatus.FORBIDDEN);
    verifyNoInteractions(cloudCredentialVendor);
  }

  @Test
  public void opaReceivesThePersistedScimIdentity() {
    String usersPath = "/api/1.0/unity-control/scim2/Users";
    JsonNode created =
        json(
            request(
                "writer",
                HttpMethod.POST,
                usersPath,
                Map.of(
                    "userName",
                    "denied@example.com",
                    "displayName",
                    "Created User",
                    "active",
                    true,
                    "emails",
                    List.of(
                        Map.of("value", "unused@example.com", "primary", false),
                        Map.of("value", "created@example.com", "primary", true)))),
            201);
    assertThat(repositories.getUserRepository().getUserByEmail("created@example.com").getId())
        .isEqualTo(created.path("id").asText());

    assertThat(
            request(
                    "writer",
                    HttpMethod.POST,
                    usersPath,
                    Map.of(
                        "userName",
                        "created@example.com",
                        "emails",
                        List.of(Map.of("value", "denied@example.com", "primary", true))))
                .status())
        .isEqualTo(HttpStatus.FORBIDDEN);
    assertThatThrownBy(() -> repositories.getUserRepository().getUserByEmail("denied@example.com"))
        .isInstanceOf(BaseException.class);
  }

  @Test
  public void opaReceivesDeltaRenameDestination() {
    Map<String, String> ids = createStorageResources();
    JsonNode table = createExternalTable("table");
    ids.put("table", table.path("table_id").asText());
    setPolicyData(ids);
    String tablePath = UC + "/delta/v1/catalogs/visible/schemas/schema/tables/table/rename";
    assertThat(request("writer", HttpMethod.POST, tablePath, Map.of("new-name", "denied")).status())
        .isEqualTo(HttpStatus.FORBIDDEN);
    assertThat(repositories.getTableRepository().getTable("visible.schema.table").getTableId())
        .isEqualTo(ids.get("table"));
    assertThat(
            request("writer", HttpMethod.POST, tablePath, Map.of("new-name", "renamed")).status())
        .isEqualTo(HttpStatus.NO_CONTENT);
    assertThat(repositories.getTableRepository().getTable("visible.schema.renamed").getTableId())
        .isEqualTo(ids.get("table"));
  }

  @Test
  public void nativePermissionApisAreUnavailableInOpaMode() {
    createCatalogAndSchema();
    json(request("admin", HttpMethod.GET, UC + "/permissions/catalog/visible", null), 501);
    json(
        request(
            "admin",
            HttpMethod.PATCH,
            UC + "/permissions/catalog/visible",
            Map.of("changes", List.of())),
        501);
  }

  private Map<String, String> createStorageResources() {
    Map<String, String> ids = createCatalogAndSchema();
    json(
        request(
            "admin",
            HttpMethod.POST,
            UC + "/credentials",
            Map.of(
                "name",
                "credential",
                "purpose",
                "STORAGE",
                "aws_iam_role",
                Map.of("role_arn", "arn:aws:iam::123456789012:role/test-role"))),
        200);
    JsonNode location =
        json(
            request(
                "admin",
                HttpMethod.POST,
                UC + "/external-locations",
                Map.of("name", "location", "url", CLOUD_ROOT, "credential_name", "credential")),
            200);
    ids.put("external_location", location.path("id").asText());
    return ids;
  }

  private Map<String, String> createCatalogAndSchema() {
    JsonNode catalog =
        json(request("admin", HttpMethod.POST, UC + "/catalogs", Map.of("name", "visible")), 200);
    JsonNode schema =
        json(
            request(
                "admin",
                HttpMethod.POST,
                UC + "/schemas",
                Map.of("name", "schema", "catalog_name", "visible")),
            200);
    return new HashMap<>(
        Map.of(
            "catalog", catalog.path("id").asText(), "schema", schema.path("schema_id").asText()));
  }

  private JsonNode createExternalTable(String name) {
    return json(request("admin", HttpMethod.POST, UC + "/tables", externalTable(name)), 200);
  }

  private Map<String, Object> externalTable(String name) {
    return Map.of(
        "name",
        name,
        "catalog_name",
        "visible",
        "schema_name",
        "schema",
        "table_type",
        "EXTERNAL",
        "data_source_format",
        "PARQUET",
        "storage_location",
        CLOUD_ROOT + "/table",
        "columns",
        List.of());
  }

  private AggregatedHttpResponse pathCredentials(String caller, String url, String operation) {
    return request(
        caller,
        HttpMethod.POST,
        UC + "/temporary-path-credentials",
        Map.of("url", url, "operation", operation));
  }

  @SneakyThrows
  private AggregatedHttpResponse request(
      String caller, HttpMethod method, String path, Object body) {
    String email = caller.equals("admin") ? caller : caller + "@example.com";
    String token =
        JWT.create()
            .withSubject(email)
            .withIssuer(INTERNAL)
            .withIssuedAt(new Date())
            .withKeyId(securityConfiguration.getKeyId())
            .withJWTId(UUID.randomUUID().toString())
            .sign(securityConfiguration.algorithmRSA());
    var request =
        WebClient.builder(serverConfig.getServerUrl())
            .auth(AuthToken.ofOAuth2(token))
            .build()
            .prepare()
            .method(method)
            .path(path);
    if (body != null) {
      request.content(MediaType.JSON, MAPPER.writeValueAsString(body));
    }
    return request.execute().aggregate().join();
  }

  @SneakyThrows
  private static JsonNode json(AggregatedHttpResponse response, int status) {
    assertThat(response.status().code()).as(response.contentUtf8()).isEqualTo(status);
    return MAPPER.readTree(response.contentUtf8());
  }

  @SneakyThrows
  private static void setPolicyData(Map<String, String> ids) {
    AggregatedHttpResponse response =
        WebClient.of(opaUrl())
            .prepare()
            .put("/v1/data/test")
            .content(MediaType.JSON, MAPPER.writeValueAsString(ids))
            .execute()
            .aggregate()
            .join();
    assertThat(response.status()).isEqualTo(HttpStatus.NO_CONTENT);
  }
}
