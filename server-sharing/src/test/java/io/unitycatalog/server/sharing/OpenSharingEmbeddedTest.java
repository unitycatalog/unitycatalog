package io.unitycatalog.server.sharing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.io.IOException;
import java.net.URLEncoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Embedded OpenSharing on a UC server with authorization on, driven over HTTP. */
public class OpenSharingEmbeddedTest extends BaseAuthCRUDTest {

  private static final String UC = "/api/2.1/unity-catalog";
  private static final String PROVIDER = UC + "/opensharing/provider";
  private static final String ALICE = "alice@example.com";
  private static final String ADD_ORDERS =
      "{\"updates\": [{\"action\": \"ADD\", \"dataObject\": {\"name\": \"main.sales.orders\","
          + " \"type\": \"TABLE\", \"sharedAs\": \"sales.orders\"}}]}";
  private static final String COMMENT = "{\"comment\": \"x\"}";
  private static final ObjectMapper JSON = new ObjectMapper();

  private WebClient client;
  private String admin;
  private String alice;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.OPENSHARING_ENABLED.getKey(), "true");
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    client = WebClient.of(serverConfig.getServerUrl());
    admin = securityContext.getServiceToken();
    createEnabledUser(ALICE);
    alice = securityContext.createAccessToken(ALICE, Duration.ofHours(1));
    expect(200, admin, HttpMethod.POST, UC + "/catalogs", "{\"name\": \"main\"}");
    expect(
        200,
        admin,
        HttpMethod.POST,
        UC + "/schemas",
        "{\"name\": \"sales\", \"catalog_name\": \"main\"}");
    String column =
        "{\"name\": \"id\", \"type_text\": \"bigint\", \"type_name\": \"LONG\", \"position\": 0,"
            + " \"nullable\": true, \"type_json\":"
            + " \"{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,"
            + "\\\"metadata\\\":{}}\"}";
    expect(
        200,
        admin,
        HttpMethod.POST,
        UC + "/tables",
        "{\"name\": \"orders\", \"catalog_name\": \"main\", \"schema_name\": \"sales\","
            + " \"table_type\": \"EXTERNAL\", \"data_source_format\": \"DELTA\","
            + " \"storage_location\": \""
            + testDirectoryRoot.resolve("orders").toUri()
            + "\", \"columns\": ["
            + column
            + "]}");
  }

  @Test
  public void providerApiRequiresUnityCatalogPrivileges() {
    expect(401, null, HttpMethod.GET, PROVIDER + "/shares", null);
    JsonNode malformed = expect(401, "not-a-jwt", HttpMethod.GET, PROVIDER + "/shares", null);
    assertThat(malformed.path("errorCode").asText()).isEqualTo("UNAUTHENTICATED");

    expect(403, alice, HttpMethod.POST, PROVIDER + "/shares", share("alice_share"));
    grant("metastore", "metastore", "CREATE SHARE");
    expect(201, alice, HttpMethod.POST, PROVIDER + "/shares", share("alice_share"));

    // Sharing a table takes the access UC asks for before vending its read credentials.
    expect(403, alice, HttpMethod.PATCH, PROVIDER + "/shares/alice_share", ADD_ORDERS);
    grant("catalog", "main", "USE CATALOG");
    grant("schema", "main.sales", "USE SCHEMA");
    expect(403, alice, HttpMethod.PATCH, PROVIDER + "/shares/alice_share", ADD_ORDERS);
    grant("table", "main.sales.orders", "SELECT");
    expect(200, alice, HttpMethod.PATCH, PROVIDER + "/shares/alice_share", ADD_ORDERS);

    // Other users' shares take USE SHARE to read.
    expect(201, admin, HttpMethod.POST, PROVIDER + "/shares", share("admin_share"));
    assertThat(names(expect(200, alice, HttpMethod.GET, PROVIDER + "/shares", null)))
        .containsExactly("alice_share");
    expectDenied(alice, HttpMethod.GET, PROVIDER + "/shares/admin_share", null);
    grant("metastore", "metastore", "USE SHARE");
    assertThat(names(expect(200, alice, HttpMethod.GET, PROVIDER + "/shares", null)))
        .containsExactlyInAnyOrder("alice_share", "admin_share");
    expect(200, alice, HttpMethod.GET, PROVIDER + "/shares/admin_share", null);

    // It doesn't let alice change one: that takes ownership, which UC checks first.
    String adminShare = PROVIDER + "/shares/admin_share";
    expectDenied(alice, HttpMethod.PATCH, adminShare, COMMENT);
    expectDenied(alice, HttpMethod.DELETE, adminShare, null);
  }

  @Test
  public void unityCatalogRecordsShareOwners() {
    grant("metastore", "metastore", "CREATE SHARE");
    expect(201, alice, HttpMethod.POST, PROVIDER + "/shares", share("alice_share"));

    // Reading one takes ownership, which UC records as OpenSharing creates it, or what the
    // metastore owner holds. Names resolve in any case, as OpenSharing's do.
    for (String token : List.of(alice, admin)) {
      expect(200, token, HttpMethod.GET, PROVIDER + "/shares/Alice_Share", null);
    }
    JsonNode missing = expect(404, alice, HttpMethod.GET, PROVIDER + "/shares/nope", null);
    assertThat(missing.path("errorCode").asText()).isEqualTo("RESOURCE_DOES_NOT_EXIST");

    // Changing one takes ownership alone.
    expect(200, alice, HttpMethod.PATCH, PROVIDER + "/shares/alice_share", COMMENT);
    expectDenied(admin, HttpMethod.PATCH, PROVIDER + "/shares/alice_share", COMMENT);

    // Ownership is recorded by id, so a share made again under the same name is its new
    // creator's alone.
    expect(204, alice, HttpMethod.DELETE, PROVIDER + "/shares/alice_share", null);
    expect(201, admin, HttpMethod.POST, PROVIDER + "/shares", share("alice_share"));
    expectDenied(alice, HttpMethod.GET, PROVIDER + "/shares/alice_share", null);
    expectDenied(alice, HttpMethod.DELETE, PROVIDER + "/shares/alice_share", null);
  }

  @Test
  public void listingsAreFilteredPerItemAfterPaging() {
    grant("metastore", "metastore", "CREATE SHARE");
    expect(201, admin, HttpMethod.POST, PROVIDER + "/shares", share("a_share"));
    expect(201, admin, HttpMethod.POST, PROVIDER + "/shares", share("b_share"));
    expect(201, alice, HttpMethod.POST, PROVIDER + "/shares", share("c_share"));

    // As in UC's own listings, a page is filtered after it is read, so alice's only share comes
    // on the last of three one-share pages.
    List<List<String>> pages = new ArrayList<>();
    String token = null;
    do {
      String query = token == null ? "" : "&pageToken=" + URLEncoder.encode(token, UTF_8);
      JsonNode page =
          expect(200, alice, HttpMethod.GET, PROVIDER + "/shares?maxResults=1" + query, null);
      pages.add(names(page));
      token = page.path("nextPageToken").isTextual() ? page.path("nextPageToken").asText() : null;
    } while (token != null);
    assertThat(pages).containsExactly(List.of(), List.of(), List.of("c_share"));
  }

  private static String share(String name) {
    return "{\"name\": \"" + name + "\"}";
  }

  private void grant(String securableType, String fullName, String privilege) {
    permissions(securableType, fullName, "add", privilege);
  }

  private void permissions(String securableType, String fullName, String change, String privilege) {
    expect(
        200,
        admin,
        HttpMethod.PATCH,
        UC + "/permissions/" + securableType + "/" + fullName,
        "{\"changes\": [{\"principal\": \"%s\", \"%s\": [\"%s\"]}]}"
            .formatted(ALICE, change, privilege));
  }

  /** Expects UC to deny a request by its method's expression, before OpenSharing sees it. */
  private void expectDenied(String token, HttpMethod method, String path, String body) {
    assertThat(expect(403, token, method, path, body).path("message").asText())
        .as("%s %s", method, path)
        .isEqualTo("Access denied.");
  }

  private JsonNode expect(int status, String token, HttpMethod method, String path, String body) {
    AggregatedHttpResponse response = call(token, method, path, body);
    assertThat(response.status().code())
        .as("%s %s: %s", method, path, response.contentUtf8())
        .isEqualTo(status);
    try {
      return response.content().isEmpty()
          ? JSON.createObjectNode()
          : JSON.readTree(response.contentUtf8());
    } catch (IOException e) {
      return fail(method + " " + path + " returned " + response.contentUtf8(), e);
    }
  }

  private AggregatedHttpResponse call(String token, HttpMethod method, String path, String body) {
    RequestHeadersBuilder headers = RequestHeaders.builder(method, path);
    if (token != null) {
      headers.add(HttpHeaderNames.AUTHORIZATION, "Bearer " + token);
    }
    if (body == null) {
      return client.execute(headers.build()).aggregate().join();
    }
    headers.contentType(MediaType.JSON);
    return client.execute(headers.build(), HttpData.ofUtf8(body)).aggregate().join();
  }

  private static List<String> names(JsonNode page) {
    List<String> names = new ArrayList<>();
    page.path("items").forEach(item -> names.add(item.path("name").asText()));
    return names;
  }
}
