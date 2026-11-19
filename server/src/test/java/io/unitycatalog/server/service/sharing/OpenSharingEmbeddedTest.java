package io.unitycatalog.server.service.sharing;

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
    serverProperties.setProperty(
        Property.OPENSHARING_PUBLIC_URL.getKey(), "https://sharing.example.com");
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    client = WebClient.of(serverConfig.getServerUrl());
    admin = securityContext.getServiceToken();
    createEnabledUser(ALICE);
    alice = securityContext.createAccessToken(ALICE, Duration.ofHours(1));
    // Admin creates catalog main.
    expect(200, admin, HttpMethod.POST, UC + "/catalogs", "{\"name\": \"main\"}");
    // Admin creates schema main.sales.
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
    // Admin creates the Delta table main.sales.orders.
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
    // A request without a token is unauthenticated.
    expect(401, null, HttpMethod.GET, PROVIDER + "/shares", null);
    // A malformed token is UNAUTHENTICATED too.
    JsonNode malformed = expect(401, "not-a-jwt", HttpMethod.GET, PROVIDER + "/shares", null);
    assertThat(malformed.path("errorCode").asText()).isEqualTo("UNAUTHENTICATED");

    // Alice can't create a share without CREATE SHARE.
    expect(403, alice, HttpMethod.POST, PROVIDER + "/shares", share("alice_share"));
    // CREATE SHARE on the metastore lets alice create a share.
    grant("metastore", "metastore", "CREATE SHARE");
    // Alice creates her share.
    expect(201, alice, HttpMethod.POST, PROVIDER + "/shares", share("alice_share"));

    // Sharing a table takes the access UC asks for before vending its read credentials.
    expect(403, alice, HttpMethod.PATCH, PROVIDER + "/shares/alice_share", ADD_ORDERS);
    // USE CATALOG and USE SCHEMA still don't let alice add the table.
    grant("catalog", "main", "USE CATALOG");
    // Alice gets USE SCHEMA on main.sales.
    grant("schema", "main.sales", "USE SCHEMA");
    // Without SELECT, alice still can't add the table.
    expect(403, alice, HttpMethod.PATCH, PROVIDER + "/shares/alice_share", ADD_ORDERS);
    // SELECT on the table lets alice add it to her share.
    grant("table", "main.sales.orders", "SELECT");
    // Alice adds the table to her share.
    expect(200, alice, HttpMethod.PATCH, PROVIDER + "/shares/alice_share", ADD_ORDERS);

    // Other users' shares take USE SHARE to read.
    // Admin creates a share that alice doesn't own.
    expect(201, admin, HttpMethod.POST, PROVIDER + "/shares", share("admin_share"));
    // Without USE SHARE, alice lists only her own share.
    assertThat(names(expect(200, alice, HttpMethod.GET, PROVIDER + "/shares", null)))
        .containsExactly("alice_share");
    // Without USE SHARE, alice can't read admin's share.
    expectDenied(alice, HttpMethod.GET, PROVIDER + "/shares/admin_share", null);
    // USE SHARE lets alice list and read admin's share.
    grant("metastore", "metastore", "USE SHARE");
    // Alice now lists both shares.
    assertThat(names(expect(200, alice, HttpMethod.GET, PROVIDER + "/shares", null)))
        .containsExactlyInAnyOrder("alice_share", "admin_share");
    // Alice can now read admin's share.
    expect(200, alice, HttpMethod.GET, PROVIDER + "/shares/admin_share", null);

    // It doesn't let alice change one: that takes ownership, which UC checks first.
    String adminShare = PROVIDER + "/shares/admin_share";
    // Alice can't update admin's share even with USE SHARE.
    expectDenied(alice, HttpMethod.PATCH, adminShare, COMMENT);
    // Alice can't delete admin's share even with USE SHARE.
    expectDenied(alice, HttpMethod.DELETE, adminShare, null);
  }

  @Test
  public void unityCatalogRecordsShareOwners() {
    // CREATE SHARE on the metastore lets alice create a share.
    grant("metastore", "metastore", "CREATE SHARE");
    // Alice creates her share.
    expect(201, alice, HttpMethod.POST, PROVIDER + "/shares", share("alice_share"));

    // Reading one takes ownership, which UC records as OpenSharing creates it, or what the
    // metastore owner holds. Names resolve in any case, as OpenSharing's do.
    for (String token : List.of(alice, admin)) {
      // The owner and the metastore owner read the share by a differently cased name.
      expect(200, token, HttpMethod.GET, PROVIDER + "/shares/Alice_Share", null);
    }
    // An unknown share name is OpenSharing's 404.
    JsonNode missing = expect(404, alice, HttpMethod.GET, PROVIDER + "/shares/nope", null);
    assertThat(missing.path("errorCode").asText()).isEqualTo("RESOURCE_DOES_NOT_EXIST");

    // Changing one takes ownership alone.
    expect(200, alice, HttpMethod.PATCH, PROVIDER + "/shares/alice_share", COMMENT);
    // Admin, who doesn't own the share, can't change it.
    expectDenied(admin, HttpMethod.PATCH, PROVIDER + "/shares/alice_share", COMMENT);

    // Ownership is recorded by id, so a share made again under the same name is its new
    // creator's alone.
    expect(204, alice, HttpMethod.DELETE, PROVIDER + "/shares/alice_share", null);
    // Admin recreates a share under the name alice's old share had.
    expect(201, admin, HttpMethod.POST, PROVIDER + "/shares", share("alice_share"));
    // Alice can't read the recreated share.
    expectDenied(alice, HttpMethod.GET, PROVIDER + "/shares/alice_share", null);
    // Alice can't delete the recreated share.
    expectDenied(alice, HttpMethod.DELETE, PROVIDER + "/shares/alice_share", null);
  }

  @Test
  public void adminTransfersASharesOwnership() throws IOException {
    // CREATE SHARE on the metastore lets alice create a share.
    grant("metastore", "metastore", "CREATE SHARE");
    // Alice creates her share.
    expect(201, alice, HttpMethod.POST, PROVIDER + "/shares", share("alice_share"));
    String bob = createEnabledUserReturningId("bob@example.com");
    String bobToken = securityContext.createAccessToken("bob@example.com", Duration.ofHours(1));
    String transfer = "{\"ownerId\": \"" + bob + "\"}";
    String path = PROVIDER + "/shares/alice_share";

    // A transfer takes a catalog admin, and rides on no other change. Alice owns the share, so
    // UC lets her PATCH it and OpenSharing's own admin check denies the transfer.
    assertThat(expect(403, alice, HttpMethod.PATCH, path, transfer).path("message").asText())
        .isEqualTo("only an admin of the catalog may change the owner of share 'alice_share'");
    // A transfer combined with another change is an invalid request.
    JsonNode refused =
        expect(
            400,
            admin,
            HttpMethod.PATCH,
            path,
            "{\"comment\": \"x\", \"ownerId\": \"" + bob + "\"}");
    assertThat(refused.path("errorCode").asText()).isEqualTo("INVALID_PARAMETER_VALUE");
    // A catalog admin can transfer the share's owner.
    JsonNode transferred = expect(200, admin, HttpMethod.PATCH, path, transfer);
    assertThat(transferred.path("ownerId").asText()).isEqualTo(bob);

    // UC's owner record moved with it: the new owner keeps the share, the old one loses it.
    expect(200, bobToken, HttpMethod.PATCH, path, COMMENT);
    // The previous owner can no longer change the share.
    expectDenied(alice, HttpMethod.PATCH, path, COMMENT);
  }

  @Test
  public void listingsAreFilteredPerItemAfterPaging() {
    // CREATE SHARE on the metastore lets alice create shares.
    grant("metastore", "metastore", "CREATE SHARE");
    // Admin's first share, which alice can't see.
    expect(201, admin, HttpMethod.POST, PROVIDER + "/shares", share("a_share"));
    // Admin's second share, which alice can't see.
    expect(201, admin, HttpMethod.POST, PROVIDER + "/shares", share("b_share"));
    // Alice's own share, the only one she can see.
    expect(201, alice, HttpMethod.POST, PROVIDER + "/shares", share("c_share"));

    // As in UC's own listings, a page is filtered after it is read, so alice's only share comes
    // on the last of three one-share pages.
    List<List<String>> pages = new ArrayList<>();
    String token = null;
    do {
      String query = token == null ? "" : "&pageToken=" + URLEncoder.encode(token, UTF_8);
      // Alice reads the next page of one share.
      JsonNode page =
          expect(200, alice, HttpMethod.GET, PROVIDER + "/shares?maxResults=1" + query, null);
      pages.add(names(page));
      token = page.path("nextPageToken").isTextual() ? page.path("nextPageToken").asText() : null;
    } while (token != null);
    // Alice's only share comes on the last of three pages.
    assertThat(pages).containsExactly(List.of(), List.of(), List.of("c_share"));
  }

  private static String share(String name) {
    return "{\"name\": \"" + name + "\"}";
  }

  /** Creates an ENABLED user and returns the id UC assigned it. */
  private String createEnabledUserReturningId(String email) throws IOException {
    RequestHeaders headers =
        RequestHeaders.builder(HttpMethod.POST, "/api/1.0/unity-control/scim2/Users")
            .contentType(MediaType.JSON)
            .add(HttpHeaderNames.X_REQUESTED_WITH, "XMLHttpRequest")
            .add(HttpHeaderNames.COOKIE, "UC_TOKEN=" + securityContext.getServiceToken())
            .build();
    AggregatedHttpResponse response =
        WebClient.builder(serverConfig.getServerUrl())
            .build()
            .execute(
                headers,
                HttpData.ofUtf8(
                    "{\"displayName\": \"Test User\", \"emails\": [{\"value\": \""
                        + email
                        + "\", \"primary\": true}]}"))
            .aggregate()
            .join();
    assertThat(response.status().code()).isEqualTo(201);
    return JSON.readTree(response.contentUtf8()).path("id").asText();
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
