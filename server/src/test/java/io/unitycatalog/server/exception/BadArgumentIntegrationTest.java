package io.unitycatalog.server.exception;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * End-to-end pin of the bad-user-input wire contract. Each case sends a concrete malformed request
 * and asserts the full 400 INVALID_ARGUMENT response body byte-for-byte against an expected value.
 * Anything that adds fields, drops fields, changes the error code, changes the message, or leaks
 * new internal details will fail in CI.
 *
 * <p>Runs with {@code INCLUDE_STACK_TRACE_IN_ERROR=false}: the production wire shape external
 * clients see, without the environment-dependent {@code stack_trace} field.
 *
 * <p>All cases run in a single test method because {@link BaseServerTest} starts a fresh Unity
 * Catalog server per {@code @Test} via {@code @BeforeEach}; splitting would multiply setup cost
 * ~6x. Case-by-case assertion failures short-circuit the remainder, which is acceptable for a tight
 * wire-contract pin.
 *
 * <p>The body-parse cases exercise {@link TypedErrorJacksonRequestConverter} {@code ->} {@link
 * MalformedRequestBodyException} {@code ->} {@link BaseExceptionHandler}. The {@code @Param} cases
 * exercise Armeria's {@code AnnotatedValueResolver} directly; for those, the wire contract has been
 * main's behavior for some time and is pinned here so future changes don't regress it silently. The
 * DRC case additionally pins the Delta error-envelope wrapping done by {@code
 * DeltaRestExceptionHandler}.
 */
public class BadArgumentIntegrationTest extends BaseServerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String TABLES_PATH = "/api/2.1/unity-catalog/tables";
  private static final String DRC_CONFIG_PATH = "/api/2.1/unity-catalog/delta/v1/config";

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.INCLUDE_STACK_TRACE_IN_ERROR.getKey(), "false");
  }

  @Test
  public void badInputReturns400() throws Exception {
    // ---- Body-parse path: TypedErrorJacksonRequestConverter -> MalformedRequestBodyException ----
    assertUcInvalidArgument(
        post(TABLES_PATH, "{not valid json"),
        "Malformed request body: Unexpected character ('n' (code 110)): "
            + "was expecting double-quote to start field name");

    // Jackson's message surfaces the target class FQCN verbatim. UC's model package is part of
    // the public OSS wire contract, not a secret; pinning it exactly means any future change to
    // widen or narrow the leaked detail fails here and forces a conscious decision.
    assertUcInvalidArgument(
        post(TABLES_PATH, "\"this-is-a-string-not-an-object\""),
        "Malformed request body: Cannot construct instance of "
            + "`io.unitycatalog.server.model.CreateTable` (although at least one Creator exists): "
            + "no String-argument constructor/factory method to deserialize from String value "
            + "('this-is-a-string-not-an-object')");

    // ---- @Param conversion path: Armeria AnnotatedValueResolver -> IllegalArgumentException ----
    assertUcInvalidArgument(
        get(TABLES_PATH + "?catalog_name=x&schema_name=y&max_results=not-an-int"),
        "For input string: \"not-an-int\"");

    assertUcInvalidArgument(
        get(TABLES_PATH + "?catalog_name=x&schema_name=y&omit_properties=maybe"),
        "Can't convert 'maybe' to type 'Boolean'.");

    assertUcInvalidArgument(
        get(TABLES_PATH + "?schema_name=y"), "Mandatory parameter/header is missing: catalog_name");

    // ---- DRC path: DeltaRestExceptionHandler wraps in the Delta error-response envelope ----
    // DRC inherits toBaseException from BaseExceptionHandler, then createErrorResponse builds the
    // Delta-spec shape (error -> message/type/code/stack). The only current DRC bad-input surface
    // is missing @Param; both endpoints take only String params, loadTable's args are path
    // variables, so there is no type-conversion path to exercise.
    assertDeltaInvalidArgument(
        get(DRC_CONFIG_PATH), "Mandatory parameter/header is missing: catalog");
  }

  private void assertUcInvalidArgument(HttpResponse<String> resp, String expectedMessage)
      throws Exception {
    assertThat(resp.statusCode()).isEqualTo(400);
    assertThat(resp.headers().firstValue("content-type").orElse("")).startsWith("application/json");
    JsonNode expected =
        MAPPER.valueToTree(
            Map.of(
                "error_code",
                "INVALID_ARGUMENT",
                "details",
                List.of(Map.of("reason", "INVALID_ARGUMENT", "@type", "google.rpc.ErrorInfo")),
                "message",
                expectedMessage));
    assertThat(MAPPER.readTree(resp.body()))
        .as("UC response body should match the documented error contract exactly")
        .isEqualTo(expected);
  }

  private void assertDeltaInvalidArgument(HttpResponse<String> resp, String expectedMessage)
      throws Exception {
    assertThat(resp.statusCode()).isEqualTo(400);
    assertThat(resp.headers().firstValue("content-type").orElse("")).startsWith("application/json");
    // Map.of rejects null values; ErrorModel.stack serializes as null when stack traces are off.
    Map<String, Object> error = new LinkedHashMap<>();
    error.put("message", expectedMessage);
    error.put("type", "InvalidParameterValueException");
    error.put("code", 400);
    error.put("stack", null);
    JsonNode expected = MAPPER.valueToTree(Map.of("error", error));
    assertThat(MAPPER.readTree(resp.body()))
        .as("Delta response body should match the Delta error envelope exactly")
        .isEqualTo(expected);
  }

  private HttpResponse<String> post(String path, String body) throws Exception {
    return HttpClient.newHttpClient()
        .send(
            HttpRequest.newBuilder(URI.create(serverConfig.getServerUrl() + path))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer ")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> get(String path) throws Exception {
    return HttpClient.newHttpClient()
        .send(
            HttpRequest.newBuilder(URI.create(serverConfig.getServerUrl() + path))
                .header("Authorization", "Bearer ")
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString());
  }
}
