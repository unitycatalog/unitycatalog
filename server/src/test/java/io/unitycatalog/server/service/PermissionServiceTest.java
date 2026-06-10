package io.unitycatalog.server.service;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import io.unitycatalog.server.sdk.access.SdkAccessControlBaseCRUDTest;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link PermissionService} with authorization enabled.
 *
 * <p>These tests follow the same approach as {@code AuthServiceTest}: a real Unity Catalog server
 * is booted (via {@link SdkAccessControlBaseCRUDTest}) with {@code server.authorization=enable} and
 * the permission endpoints are exercised end-to-end with a raw Armeria {@link WebClient}. Unlike a
 * Mockito unit test, this also covers the Armeria authentication ({@code AuthDecorator}) and
 * authorization ({@code UnityAccessDecorator}) decorators that wrap the service.
 */
public class PermissionServiceTest extends SdkAccessControlBaseCRUDTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String BASE_PATH = "/api/2.1/unity-catalog/";

  private WebClient client;

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    client = WebClient.builder(serverConfig.getServerUrl()).build();
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  @Test
  @SneakyThrows
  public void ownerCanGrantAndReadCatalogPermissions() {
    createTestUser(REGULAR_1);

    // Admin (metastore owner) grants USE CATALOG to regular-1.
    AggregatedHttpResponse grantResponse =
        updateCatalogPermissions(
            CATALOG_NAME, adminConfig.getAuthToken(), addPrivilegeBody(REGULAR_1, "USE CATALOG"));
    assertThat(grantResponse.status()).isEqualTo(HttpStatus.OK);
    assertThat(privilegesFor(parse(grantResponse), REGULAR_1)).containsExactly("USE CATALOG");

    // Reading back as the owner exposes the full ACL, including regular-1's grant.
    AggregatedHttpResponse getResponse =
        getCatalogPermissions(CATALOG_NAME, adminConfig.getAuthToken());
    assertThat(getResponse.status()).isEqualTo(HttpStatus.OK);
    assertThat(privilegesFor(parse(getResponse), REGULAR_1)).containsExactly("USE CATALOG");
  }

  @Test
  @SneakyThrows
  public void nonOwnerCannotUpdateCatalogPermissions() {
    createTestUser(REGULAR_1);

    // regular-1 is neither metastore nor catalog owner, so the decorator rejects the update.
    AggregatedHttpResponse response =
        updateCatalogPermissions(
            CATALOG_NAME,
            createTestJwtToken(REGULAR_1),
            addPrivilegeBody(REGULAR_1, "USE CATALOG"));
    assertThat(response.status()).isEqualTo(HttpStatus.FORBIDDEN);
  }

  @Test
  @SneakyThrows
  public void nonOwnerCanReadCatalogPermissions() {
    createTestUser(REGULAR_1);

    // GET is gated only on authentication, so a non-owner gets their own (empty) view.
    AggregatedHttpResponse response =
        getCatalogPermissions(CATALOG_NAME, createTestJwtToken(REGULAR_1));
    assertThat(response.status()).isEqualTo(HttpStatus.OK);
    assertThat(privilegesFor(parse(response), REGULAR_1)).isEmpty();
  }

  @Test
  public void unauthenticatedRequestIsRejected() {
    // No bearer token -> the AuthDecorator rejects the request before it reaches the service.
    AggregatedHttpResponse response = getCatalogPermissions(CATALOG_NAME, null);
    assertThat(response.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
  }

  // ---------------------------------------------------------------------------
  // Request helpers
  // ---------------------------------------------------------------------------

  private RequestHeaders headers(HttpMethod method, String path, String token) {
    RequestHeadersBuilder builder =
        RequestHeaders.builder().method(method).path(path).contentType(MediaType.JSON);
    if (token != null) {
      builder.add(HttpHeaderNames.AUTHORIZATION, "Bearer " + token);
    }
    return builder.build();
  }

  private AggregatedHttpResponse send(HttpMethod method, String path, String token, String body) {
    if (body == null) {
      return client.execute(headers(method, path, token)).aggregate().join();
    }
    return client.execute(headers(method, path, token), HttpData.ofUtf8(body)).aggregate().join();
  }

  private AggregatedHttpResponse getCatalogPermissions(String catalogName, String token) {
    return send(HttpMethod.GET, BASE_PATH + "permissions/catalog/" + catalogName, token, null);
  }

  private AggregatedHttpResponse updateCatalogPermissions(
      String catalogName, String token, String body) {
    return send(HttpMethod.PATCH, BASE_PATH + "permissions/catalog/" + catalogName, token, body);
  }

  private static String addPrivilegeBody(String principal, String privilege) {
    return String.format(
        "{\"changes\":[{\"principal\":\"%s\",\"add\":[\"%s\"],\"remove\":[]}]}",
        principal, privilege);
  }

  @SneakyThrows
  private static JsonNode parse(AggregatedHttpResponse response) {
    return MAPPER.readTree(response.contentUtf8());
  }

  /** Returns the privilege strings assigned to {@code principal} in a PermissionsList response. */
  private static List<String> privilegesFor(JsonNode permissionsList, String principal) {
    List<String> privileges = new ArrayList<>();
    JsonNode assignments = permissionsList.get("privilege_assignments");
    if (assignments == null) {
      return privileges;
    }
    for (JsonNode assignment : assignments) {
      if (principal.equals(assignment.get("principal").asText())) {
        assignment.get("privileges").forEach(node -> privileges.add(node.asText()));
      }
    }
    return privileges;
  }
}
