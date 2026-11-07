package io.unitycatalog.server.service.sharing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Embedded OpenSharing on a UC server with authorization on, driven over HTTP. Reading table data
 * takes cloud storage, which OpenSharing vends credentials for, so these cover the API down to
 * resolving the shared table.
 */
public class OpenSharingEmbeddedTest extends BaseAuthCRUDTest {

  private static final String PUBLIC_URL = "https://sharing.example.com";
  private static final String UC = "/api/2.1/unity-catalog";
  private static final String PROVIDER = UC + "/opensharing/provider";
  private static final String PROTOCOL = "/api/1.0/opensharing";
  private static final String CONTROL = "/api/1.0/unity-control";
  private static final String ALICE = "alice@example.com";
  private static final String ADD_ORDERS =
      "{\"updates\": [{\"action\": \"ADD\", \"dataObject\": {\"name\": \"main.sales.orders\","
          + " \"type\": \"TABLE\", \"sharedAs\": \"sales.orders\"}}]}";
  private static final String COMMENT = "{\"comment\": \"x\"}";
  private static final String ROTATE_TOKEN = "{\"tokenExpirationDays\": 1}";
  private static final ObjectMapper JSON = new ObjectMapper();

  private WebClient client;
  private String admin;
  private String alice;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.OPENSHARING_ENABLED.getKey(), "true");
    serverProperties.setProperty(Property.OPENSHARING_PUBLIC_URL.getKey(), PUBLIC_URL + "/");
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
    expect(403, alice, HttpMethod.POST, PROVIDER + "/recipients", recipient("acme"));
    grant("metastore", "metastore", "CREATE SHARE");
    grant("metastore", "metastore", "CREATE RECIPIENT");
    expect(201, alice, HttpMethod.POST, PROVIDER + "/shares", share("alice_share"));
    expect(201, alice, HttpMethod.POST, PROVIDER + "/recipients", recipient("acme"));

    // Sharing a table takes the access UC asks for before vending its read credentials.
    expect(403, alice, HttpMethod.PATCH, PROVIDER + "/shares/alice_share", ADD_ORDERS);
    grant("catalog", "main", "USE CATALOG");
    grant("schema", "main.sales", "USE SCHEMA");
    expect(403, alice, HttpMethod.PATCH, PROVIDER + "/shares/alice_share", ADD_ORDERS);
    grant("table", "main.sales.orders", "SELECT");
    expect(200, alice, HttpMethod.PATCH, PROVIDER + "/shares/alice_share", ADD_ORDERS);

    // Other users' shares and recipients take USE SHARE and USE RECIPIENT to read, and granting a
    // share to another user's recipient takes USE RECIPIENT. A request grants to one recipient, so
    // one granting to two is refused before either is granted. Revoking takes nothing of the
    // recipient, so it reaches OpenSharing, which finds no grant.
    String alicePermissions = PROVIDER + "/shares/alice_share/permissions";
    expect(201, admin, HttpMethod.POST, PROVIDER + "/shares", share("admin_share"));
    expect(201, admin, HttpMethod.POST, PROVIDER + "/recipients", recipient("globex"));
    assertThat(names(expect(200, alice, HttpMethod.GET, PROVIDER + "/shares", null)))
        .containsExactly("alice_share");
    expectDenied(alice, HttpMethod.GET, PROVIDER + "/shares/admin_share", null);
    expectDenied(alice, HttpMethod.GET, PROVIDER + "/shares/admin_share/permissions", null);
    expectDenied(alice, HttpMethod.GET, PROVIDER + "/recipients/globex", null);
    expectDenied(alice, HttpMethod.PATCH, alicePermissions, shareWith("globex"));
    assertThat(
            expect(400, alice, HttpMethod.PATCH, alicePermissions, shareWith("acme", "globex"))
                .path("message")
                .asText())
        .contains("only one recipient");
    assertThat(expect(200, alice, HttpMethod.GET, alicePermissions, null).path("items")).isEmpty();
    expect(200, alice, HttpMethod.PATCH, alicePermissions, shareWith("acme"));
    expect(404, alice, HttpMethod.PATCH, alicePermissions, unshareWith("globex"));
    grant("metastore", "metastore", "USE SHARE");
    grant("metastore", "metastore", "USE RECIPIENT");
    assertThat(names(expect(200, alice, HttpMethod.GET, PROVIDER + "/shares", null)))
        .containsExactlyInAnyOrder("alice_share", "admin_share");
    expect(200, alice, HttpMethod.GET, PROVIDER + "/shares/admin_share", null);
    expect(200, alice, HttpMethod.GET, PROVIDER + "/shares/admin_share/permissions", null);
    expect(200, alice, HttpMethod.GET, PROVIDER + "/recipients/globex", null);
    expect(200, alice, HttpMethod.PATCH, alicePermissions, shareWith("globex"));

    // Neither lets alice change them: that takes ownership, which UC checks first.
    String adminShare = PROVIDER + "/shares/admin_share";
    String globex = PROVIDER + "/recipients/globex";
    expectDenied(alice, HttpMethod.PATCH, adminShare, COMMENT);
    expectDenied(alice, HttpMethod.PATCH, adminShare + "/permissions", shareWith("acme"));
    expectDenied(alice, HttpMethod.PATCH, adminShare + "/permissions", unshareWith("acme"));
    expectDenied(alice, HttpMethod.DELETE, adminShare, null);
    expectDenied(alice, HttpMethod.PATCH, globex, COMMENT);
    expectDenied(alice, HttpMethod.POST, globex + "/rotate-token", ROTATE_TOKEN);
    expectDenied(alice, HttpMethod.DELETE, globex, null);
  }

  @Test
  public void startupNamesWhatEmbeddedOpenSharingNeeds() {
    Properties withoutPublicUrl = new Properties();
    withoutPublicUrl.putAll(serverProperties);
    withoutPublicUrl.remove(Property.OPENSHARING_PUBLIC_URL.getKey());
    assertThatThrownBy(
            () ->
                UnityCatalogServer.builder()
                    .serverProperties(new ServerProperties(withoutPublicUrl))
                    .hibernateConfigurator(hibernateConfigurator)
                    .build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(Property.OPENSHARING_PUBLIC_URL.getKey());
  }

  @Test
  public void unityCatalogRecordsShareAndRecipientOwners() {
    grant("metastore", "metastore", "CREATE SHARE");
    grant("metastore", "metastore", "CREATE RECIPIENT");
    expect(201, alice, HttpMethod.POST, PROVIDER + "/shares", share("alice_share"));
    expect(201, alice, HttpMethod.POST, PROVIDER + "/recipients", recipient("acme"));

    // Reading one takes ownership, which UC records as OpenSharing creates it, or what the
    // metastore owner holds. Names resolve in any case, as OpenSharing's do.
    for (String token : List.of(alice, admin)) {
      expect(200, token, HttpMethod.GET, PROVIDER + "/shares/Alice_Share", null);
      expect(200, token, HttpMethod.GET, PROVIDER + "/recipients/acme", null);
    }
    JsonNode missing = expect(404, alice, HttpMethod.GET, PROVIDER + "/shares/nope", null);
    assertThat(missing.path("errorCode").asText()).isEqualTo("RESOURCE_DOES_NOT_EXIST");
    expect(404, alice, HttpMethod.GET, PROVIDER + "/recipients/nope", null);

    // Changing one takes ownership alone.
    expect(200, alice, HttpMethod.PATCH, PROVIDER + "/shares/alice_share", COMMENT);
    expect(200, alice, HttpMethod.PATCH, PROVIDER + "/recipients/acme", COMMENT);
    expect(201, alice, HttpMethod.POST, PROVIDER + "/recipients/acme/rotate-token", ROTATE_TOKEN);
    expectDenied(admin, HttpMethod.PATCH, PROVIDER + "/shares/alice_share", COMMENT);
    expectDenied(admin, HttpMethod.DELETE, PROVIDER + "/recipients/acme", null);

    // Ownership is recorded by id, so a share or recipient made again under the same name is
    // its new creator's alone.
    expect(204, alice, HttpMethod.DELETE, PROVIDER + "/shares/alice_share", null);
    expect(204, alice, HttpMethod.DELETE, PROVIDER + "/recipients/acme", null);
    expect(201, admin, HttpMethod.POST, PROVIDER + "/shares", share("alice_share"));
    expect(201, admin, HttpMethod.POST, PROVIDER + "/recipients", recipient("acme"));
    expectDenied(alice, HttpMethod.GET, PROVIDER + "/shares/alice_share", null);
    expectDenied(alice, HttpMethod.GET, PROVIDER + "/recipients/acme", null);
    expectDenied(alice, HttpMethod.DELETE, PROVIDER + "/shares/alice_share", null);
  }

  @Test
  public void adminTransfersASharesOwnership() throws IOException {
    grant("metastore", "metastore", "CREATE SHARE");
    expect(201, alice, HttpMethod.POST, PROVIDER + "/shares", share("alice_share"));
    String bob = createEnabledUserReturningId("bob@example.com");
    String bobToken = securityContext.createAccessToken("bob@example.com", Duration.ofHours(1));
    String transfer = "{\"ownerId\": \"" + bob + "\"}";
    String path = PROVIDER + "/shares/alice_share";

    // A transfer takes a catalog admin, and rides on no other change. Alice owns the share, so
    // UC lets her PATCH it and OpenSharing's own admin check denies the transfer.
    assertThat(expect(403, alice, HttpMethod.PATCH, path, transfer).path("message").asText())
        .isEqualTo("only an admin of the catalog may change the owner of share 'alice_share'");
    JsonNode refused =
        expect(
            400,
            admin,
            HttpMethod.PATCH,
            path,
            "{\"comment\": \"x\", \"ownerId\": \"" + bob + "\"}");
    assertThat(refused.path("errorCode").asText()).isEqualTo("INVALID_PARAMETER_VALUE");
    JsonNode transferred = expect(200, admin, HttpMethod.PATCH, path, transfer);
    assertThat(transferred.path("ownerId").asText()).isEqualTo(bob);

    // UC's owner record moved with it: the new owner keeps the share, the old one loses it.
    expect(200, bobToken, HttpMethod.PATCH, path, COMMENT);
    expectDenied(alice, HttpMethod.PATCH, path, COMMENT);
  }

  @Test
  public void adminTransfersARecipientsOwnership() throws IOException {
    grant("metastore", "metastore", "CREATE RECIPIENT");
    expect(201, alice, HttpMethod.POST, PROVIDER + "/recipients", recipient("acme"));
    String bob = createEnabledUserReturningId("bob@example.com");
    String bobToken = securityContext.createAccessToken("bob@example.com", Duration.ofHours(1));
    String transfer = "{\"ownerId\": \"" + bob + "\"}";
    String path = PROVIDER + "/recipients/acme";

    // A transfer takes a catalog admin, and rides on no other change. Alice owns the recipient,
    // so UC lets her PATCH it and OpenSharing's own admin check denies the transfer.
    assertThat(expect(403, alice, HttpMethod.PATCH, path, transfer).path("message").asText())
        .isEqualTo("only an admin of the catalog may change the owner of recipient 'acme'");
    JsonNode transferred = expect(200, admin, HttpMethod.PATCH, path, transfer);
    assertThat(transferred.path("ownerId").asText()).isEqualTo(bob);

    // UC's owner record moved with it: the new owner keeps the recipient, the old one loses it.
    expect(200, bobToken, HttpMethod.PATCH, path, COMMENT);
    expect(201, bobToken, HttpMethod.POST, path + "/rotate-token", ROTATE_TOKEN);
    expectDenied(alice, HttpMethod.PATCH, path, COMMENT);
  }

  @Test
  public void listingsAreFilteredPerItemAfterPaging() {
    grant("metastore", "metastore", "CREATE SHARE");
    grant("metastore", "metastore", "CREATE RECIPIENT");
    expect(201, admin, HttpMethod.POST, PROVIDER + "/shares", share("a_share"));
    expect(201, admin, HttpMethod.POST, PROVIDER + "/shares", share("b_share"));
    expect(201, alice, HttpMethod.POST, PROVIDER + "/shares", share("c_share"));
    expect(201, admin, HttpMethod.POST, PROVIDER + "/recipients", recipient("globex"));
    expect(201, alice, HttpMethod.POST, PROVIDER + "/recipients", recipient("acme"));

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

    // Recipients carry their activation links, so other users' must be dropped too.
    assertThat(names(expect(200, alice, HttpMethod.GET, PROVIDER + "/recipients", null)))
        .containsExactly("acme");
    assertThat(names(expect(200, admin, HttpMethod.GET, PROVIDER + "/recipients", null)))
        .containsExactly("acme", "globex");
  }

  @Test
  public void recipientSeesSharedTableWhileShareOwnerCanReadIt() {
    JsonNode acme = aliceSharesOrdersWithAcme();

    // Recipients are pointed at the public url, and activate once.
    String activationUrl = acme.path("activationUrl").asText();
    assertThat(activationUrl).startsWith(PUBLIC_URL + PROTOCOL + "/activations/");
    String activation = URI.create(activationUrl).getPath();
    JsonNode profile = expect(200, null, HttpMethod.GET, activation, null);
    assertThat(profile.path("endpoint").asText()).isEqualTo(PUBLIC_URL + PROTOCOL);
    expect(404, null, HttpMethod.GET, activation, null);
    String token = profile.path("bearerToken").asText();

    String tables = PROTOCOL + "/shares/sales/schemas/sales/tables";
    assertThat(names(expect(200, token, HttpMethod.GET, PROTOCOL + "/shares", null)))
        .containsExactly("sales");
    assertThat(names(expect(200, token, HttpMethod.GET, tables, null))).containsExactly("orders");
    assertThat(expect(200, token, HttpMethod.GET, tables + "/orders", null).path("format").asText())
        .isEqualTo("delta");

    // Recipient tokens are OpenSharing's: UC does not take them, and the protocol needs one.
    expect(401, token, HttpMethod.GET, UC + "/catalogs", null);
    JsonNode anonymous = expect(401, null, HttpMethod.GET, PROTOCOL + "/shares", null);
    assertThat(anonymous.path("errorCode").asText()).isEqualTo("UNAUTHENTICATED");
    expect(401, alice, HttpMethod.GET, PROTOCOL + "/shares", null);

    // The table is resolved with the share owner's access, so a recipient loses it with them. To
    // the recipient it is gone, as if dropped.
    revoke("table", "main.sales.orders", "SELECT");
    expect(404, token, HttpMethod.GET, tables + "/orders", null);
  }

  @Test
  public void recipientLosesSharedTableOnceShareOwnerIsDeleted() {
    JsonNode acme = aliceSharesOrdersWithAcme();
    String activation = URI.create(acme.path("activationUrl").asText()).getPath();
    String token = expect(200, null, HttpMethod.GET, activation, null).path("bearerToken").asText();
    String orders = PROTOCOL + "/shares/sales/schemas/sales/tables/orders";
    expect(200, token, HttpMethod.GET, orders, null);

    // UC keeps a deleted user, disabled, and a disabled owner can't read the table either.
    String aliceId =
        expect(200, alice, HttpMethod.GET, CONTROL + "/scim2/Me", null).path("id").asText();
    expect(200, admin, HttpMethod.DELETE, CONTROL + "/scim2/Users/" + aliceId, null);
    expect(404, token, HttpMethod.GET, orders, null);
  }

  /** Alice, given the privileges to, shares main.sales.orders as sales.orders with acme. */
  private JsonNode aliceSharesOrdersWithAcme() {
    grant("metastore", "metastore", "CREATE SHARE");
    grant("metastore", "metastore", "CREATE RECIPIENT");
    grant("catalog", "main", "USE CATALOG");
    grant("schema", "main.sales", "USE SCHEMA");
    grant("table", "main.sales.orders", "SELECT");
    expect(201, alice, HttpMethod.POST, PROVIDER + "/shares", share("sales"));
    expect(200, alice, HttpMethod.PATCH, PROVIDER + "/shares/sales", ADD_ORDERS);
    JsonNode acme =
        expect(201, alice, HttpMethod.POST, PROVIDER + "/recipients", recipient("acme"));
    expect(200, alice, HttpMethod.PATCH, PROVIDER + "/shares/sales/permissions", shareWith("acme"));
    return acme;
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

  /** A share permissions change that grants SELECT to each of {@code recipients}. */
  private static String shareWith(String... recipients) {
    return Arrays.stream(recipients)
        .map("{\"recipientName\": \"%s\", \"add\": [\"SELECT\"]}"::formatted)
        .collect(Collectors.joining(", ", "{\"changes\": [", "]}"));
  }

  /** A share permissions change that revokes SELECT from {@code recipient}. */
  private static String unshareWith(String recipient) {
    return "{\"changes\": [{\"recipientName\": \"%s\", \"remove\": [\"SELECT\"]}]}"
        .formatted(recipient);
  }

  private static String recipient(String name) {
    return "{\"name\": \""
        + name
        + "\", \"authenticationType\": \"TOKEN\", \"tokenExpirationDays\": 1}";
  }

  private void grant(String securableType, String fullName, String privilege) {
    permissions(securableType, fullName, "add", privilege);
  }

  private void revoke(String securableType, String fullName, String privilege) {
    permissions(securableType, fullName, "remove", privilege);
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
