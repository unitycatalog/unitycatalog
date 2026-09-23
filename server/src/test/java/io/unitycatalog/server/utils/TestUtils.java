package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.DeltaApiException;
import io.unitycatalog.client.delta.model.DeltaErrorType;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.exception.ErrorCode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.function.Executable;

public class TestUtils {
  public static final String CATALOG_NAME = "uc_testcatalog";
  public static final String SCHEMA_NAME = "uc_testschema";
  public static final String CATALOG_NAME2 = "uc_testcatalog2";
  public static final String SCHEMA_NAME2 = "uc_testschema2";
  public static final String TABLE_NAME = "uc_testtable";
  public static final String VOLUME_NAME = "uc_testvolume";
  public static final String FUNCTION_NAME = "uc_testfunction";
  public static final String MODEL_NAME = "uc_testmodel";
  public static final String MODEL_NEW_NAME = "uc_newtestmodel";
  public static final String SCHEMA_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NAME;
  public static final String SCHEMA_NEW_NAME = "uc_newtestschema";
  public static final String SCHEMA_NEW_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NEW_NAME;
  public static final String SCHEMA_NEW_COMMENT = "new test comment";
  public static final String TABLE_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NAME + "." + TABLE_NAME;
  public static final String VOLUME_FULL_NAME =
      CATALOG_NAME + "." + SCHEMA_NAME + "." + VOLUME_NAME;
  public static final String FUNCTION_FULL_NAME =
      CATALOG_NAME + "." + SCHEMA_NAME + "." + FUNCTION_NAME;
  public static final String MODEL_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NAME + "." + MODEL_NAME;
  public static final String MODEL_NEW_FULL_NAME =
      CATALOG_NAME + "." + SCHEMA_NAME + "." + MODEL_NEW_NAME;
  public static final String COMMENT = "test comment";
  public static final String COMMENT2 = "test comment 2";
  public static final String CATALOG_NEW_NAME = "uc_newtestcatalog";
  public static final String CATALOG_NEW_COMMENT = "new test comment";
  public static final String MODEL_NEW_COMMENT = "new test model comment";
  public static final String VOLUME_NEW_NAME = "uc_newtestvolume";
  public static final String VOLUME_NEW_FULL_NAME =
      CATALOG_NAME + "." + SCHEMA_NAME + "." + VOLUME_NEW_NAME;
  public static final String MV_COMMENT = "model version comment";
  public static final String MV_SOURCE = "model version source";
  public static final String MV_RUNID = "model version runId";
  public static final String MV_SOURCE2 = "model version source 2";
  public static final String MV_RUNID2 = "model version runId 2";
  public static final String TEST_AWS_MASTER_ROLE_ARN =
      "arn:aws:iam::1234567:role/UCMasterRole-EXAMPLE";
  public static final String TEST_AWS_MASTER_ROLE_ACCESS_KEY = "masterRoleAccessKey";
  public static final String TEST_AWS_MASTER_ROLE_SECRET_KEY = "masterRoleSecretKey";
  public static final String TEST_AWS_REGION = "us-west-2";

  public static final Map<String, String> PROPERTIES =
      new HashMap<>(Map.of("prop1", "value1", "prop2", "value2"));
  public static final Map<String, String> NEW_PROPERTIES =
      new HashMap<>(Map.of("prop2", "value22", "prop3", "value33"));
  public static final String COMMON_ENTITY_NAME = "zz_uc_common_entity_name";

  public static ApiClient createApiClient(ServerConfig serverConfig) {
    URI uri = URI.create(serverConfig.getServerUrl());
    String token = serverConfig.getAuthToken() != null ? serverConfig.getAuthToken() : "";
    return ApiClientBuilder.create()
        .uri(uri)
        .tokenProvider(TokenProvider.create(Map.of("type", "static", "token", token)))
        .retryPolicy(JitterDelayRetryPolicy.builder().maxAttempts(1).build())
        .build();
  }

  /**
   * Asserts the call fails with a Unity Catalog REST error: the HTTP status matches {@code
   * errorCode}, the body is the UC envelope carrying that code, and its message contains {@code
   * containsMessage}. For Delta API endpoints use {@link #assertDeltaApiException}; for a
   * client-side SDK exception raised before the server is reached (no response body) use {@link
   * #assertClientException}.
   */
  public static void assertApiException(
      Executable executable, ErrorCode errorCode, String containsMessage) {
    ApiException ex = assertThrows(ApiException.class, executable);
    assertUcErrorEnvelope(
        ex.getCode(), ex.getResponseBody(), errorCode, Optional.of(containsMessage));
  }

  /**
   * Asserts {@code ex} carries the Unity Catalog error envelope for {@code errorCode} -- {@code
   * {"error_code": <CODE>, "message": ..., "details": [{"reason": <CODE>, ...}]}}. When {@code
   * containsMessage} is present, the message must contain it. The absence of a nested {@code
   * "error"} object is what distinguishes this from the Delta envelope.
   */
  private static void assertUcErrorEnvelope(
      int statusCode, String bodyText, ErrorCode errorCode, Optional<String> containsMessage) {
    assertThat(statusCode).isEqualTo(errorCode.getHttpStatus().code());
    JsonNode body = parseErrorBody(bodyText);
    // A UC envelope carries error_code and message at the top level, plus a details array whose
    // ErrorInfo entry repeats the code as its reason; the Delta envelope nests everything under
    // "error". Assert both the code and that it is not the Delta shape.
    assertThat(body.path("error_code").asText())
        .as("UC envelope error_code in: %s", bodyText)
        .isEqualTo(errorCode.name());
    JsonNode errorInfo = body.path("details").path(0);
    assertThat(errorInfo.path("reason").asText())
        .as("UC envelope details[0].reason in: %s", bodyText)
        .isEqualTo(errorCode.name());
    // The detail is a google.rpc.ErrorInfo packed as a protobuf Any, so its @type must be the full
    // type URL (with the type.googleapis.com/ prefix), not a bare message name.
    assertThat(errorInfo.path("@type").asText())
        .as("UC envelope details[0].@type in: %s", bodyText)
        .isEqualTo("type.googleapis.com/google.rpc.ErrorInfo");
    assertThat(body.has("error"))
        .as("expected the UC envelope, but the body is nested Delta-style: %s", bodyText)
        .isFalse();
    JsonNode message = body.path("message");
    assertThat(message.isTextual()).as("UC envelope must carry a message: %s", bodyText).isTrue();
    containsMessage.ifPresent(
        m -> assertThat(message.asText()).as("error message in: %s", bodyText).contains(m));
  }

  /**
   * Asserts a client-side SDK exception: the generated client rejected the call before reaching the
   * server, so the status matches {@code errorCode} but there is no response body to parse. Use
   * {@link #assertApiException} when the server produced the error.
   */
  public static void assertClientException(
      Executable executable, ErrorCode errorCode, String containsMessage) {
    ApiException ex = assertThrows(ApiException.class, executable);
    assertThat(ex.getCode()).isEqualTo(errorCode.getHttpStatus().code());
    assertThat(ex.getMessage()).contains(containsMessage);
  }

  /**
   * Asserts the call fails with {@code expectedStatus} and a genuinely empty body. Use only for
   * responses that carry no body by construction -- e.g. a HEAD request, which has none whatever
   * the status. A normal error response always carries a body, so assert it with {@link
   * #assertApiException} or {@link #assertDeltaApiException} instead; the empty-body check here
   * fails fast if this is misused on one.
   */
  public static void assertApiExceptionStatusOnly(Executable executable, int expectedStatus) {
    ApiException ex = assertThrows(ApiException.class, executable);
    assertThat(ex.getCode()).isEqualTo(expectedStatus);
    String bodyText = ex.getResponseBody();
    assertThat(bodyText == null || bodyText.isBlank())
        .as("expected a body-less response, but got: %s", bodyText)
        .isTrue();
  }

  public static void assertDeltaApiException(
      Executable executable, DeltaErrorType expectedType, String expectedMessageSubstring) {
    int expectedCode = ErrorCode.getDeltaHttpStatus(expectedType.getValue()).code();
    ApiException ex = assertThrows(ApiException.class, executable);
    // Check message first for better diagnostics on failure (includes the full response body)
    assertThat(ex.getMessage()).contains(expectedMessageSubstring);
    assertThat(ex.getCode()).isEqualTo(expectedCode);
    Optional<DeltaApiException> deltaExOpt = DeltaApiException.from(ex);
    assertThat(deltaExOpt)
        .as("Failed to parse Delta error response: " + ex.getResponseBody())
        .isPresent();
    DeltaApiException delta = deltaExOpt.get();
    assertThat(delta.getErrorCode()).isEqualTo(expectedCode);
    assertThat(delta.getErrorType()).isEqualTo(expectedType);
    assertThat(delta.getErrorMessage()).contains(expectedMessageSubstring);
  }

  /**
   * As {@link #assertPermissionDenied(Executable)}, and additionally asserts the error message
   * states {@code containsMessage}.
   *
   * <p>Only for when the test cares <em>why</em> permission was denied, e.g. a staging-table
   * ownership check. The envelope's own markers are asserted structurally.
   */
  public static void assertPermissionDenied(Executable executable, String containsMessage) {
    assertPermissionDeniedImpl(executable, Optional.of(containsMessage));
  }

  /**
   * Asserts the call fails with permission-denied at HTTP 403 in the <em>Unity Catalog</em> error
   * envelope, parsing the body to confirm its shape: {@code {"error_code": "PERMISSION_DENIED",
   * "message": ..., "details": [{"reason": "PERMISSION_DENIED", "@type": ...}]}}.
   *
   * <p>Use for endpoints served by {@code GlobalExceptionHandler}. Delta API endpoints speak a
   * different dialect and must be asserted with {@link #assertDeltaPermissionDenied} -- keeping the
   * two apart is what makes these tests notice if an endpoint starts answering in the wrong format.
   */
  public static void assertPermissionDenied(Executable executable) {
    assertPermissionDeniedImpl(executable, Optional.empty());
  }

  private static void assertPermissionDeniedImpl(
      Executable executable, Optional<String> containsMessage) {
    // The envelope's own PERMISSION_DENIED marker is asserted structurally below, so passing it as
    // the reason would look like a check without being one; reject it. A real reason states why.
    containsMessage.ifPresent(
        r ->
            assertThat(r)
                .as("assert the reason for the denial, not the envelope marker")
                .isNotEqualTo(ErrorCode.PERMISSION_DENIED.name()));
    ApiException ex = assertThrows(ApiException.class, executable);
    assertUcErrorEnvelope(
        ex.getCode(), ex.getResponseBody(), ErrorCode.PERMISSION_DENIED, containsMessage);
  }

  /**
   * Asserts the call fails with permission-denied in the <em>Delta API</em> error envelope, parsing
   * the body to confirm its shape: {@code {"error": {"type": "PermissionDeniedException", "code":
   * 403, "message": ...}}}.
   *
   * <p>Delta clients parse this shape specifically, so a UC-style body here would be a regression
   * even though the status code is the same. See {@link #assertPermissionDenied} for UC endpoints.
   */
  public static void assertDeltaPermissionDenied(Executable executable) {
    assertDeltaPermissionDeniedImpl(executable, Optional.empty());
  }

  /**
   * As {@link #assertDeltaPermissionDenied(Executable)}, and additionally asserts the error message
   * states {@code containsMessage}.
   *
   * <p>Only for when the test cares <em>why</em> permission was denied. The envelope's own markers
   * are asserted structurally, so passing {@code "PermissionDeniedException"} here adds nothing and
   * is rejected.
   */
  public static void assertDeltaPermissionDenied(Executable executable, String containsMessage) {
    assertDeltaPermissionDeniedImpl(executable, Optional.of(containsMessage));
  }

  private static void assertDeltaPermissionDeniedImpl(
      Executable executable, Optional<String> containsMessage) {
    int expectedCode =
        ErrorCode.getDeltaHttpStatus(DeltaErrorType.PERMISSION_DENIED_EXCEPTION.getValue()).code();
    ApiException ex = assertThrows(ApiException.class, executable);
    assertThat(ex.getCode()).isEqualTo(expectedCode);
    JsonNode body = parseErrorBody(ex.getResponseBody());
    assertThat(body.has("error"))
        .as("expected the Delta envelope nested under \"error\": %s", ex.getResponseBody())
        .isTrue();
    JsonNode error = body.path("error");
    assertThat(error.path("type").asText())
        .as("Delta envelope error.type in: %s", ex.getResponseBody())
        .isEqualTo(DeltaErrorType.PERMISSION_DENIED_EXCEPTION.getValue());
    assertThat(error.path("code").asInt())
        .as("Delta envelope error.code in: %s", ex.getResponseBody())
        .isEqualTo(expectedCode);
    // The UC envelope puts error_code at the top level; its absence distinguishes the two dialects.
    assertThat(body.has("error_code"))
        .as("expected the Delta envelope, but the body is UC-style: %s", ex.getResponseBody())
        .isFalse();
    JsonNode message = error.path("message");
    assertThat(message.isTextual())
        .as("error envelope must carry a message: %s", ex.getResponseBody())
        .isTrue();
    containsMessage.ifPresent(
        r -> {
          assertThat(r)
              .as("assert the reason for the denial, not the envelope marker")
              .isNotEqualTo(DeltaErrorType.PERMISSION_DENIED_EXCEPTION.getValue());
          assertThat(message.asText()).as("denial reason in: %s", ex.getResponseBody()).contains(r);
        });
  }

  /** Parses an error response body as JSON, failing with the raw body if it is not valid JSON. */
  private static JsonNode parseErrorBody(String bodyText) {
    try {
      return new ObjectMapper().readTree(bodyText);
    } catch (Exception e) {
      return org.assertj.core.api.Assertions.fail(
          "Error response was not valid JSON: " + bodyText, e);
    }
  }

  /**
   * Raw-HTTP counterpart to {@link #assertApiException}. Use when the generated SDK can't reach the
   * failure mode (e.g. the SDK always serializes a body, so you can't exercise body-less
   * authorization paths with it).
   */
  public static void assertHttpApiException(
      HttpResponse<String> response, ErrorCode errorCode, String containsMessage) {
    assertUcErrorEnvelope(
        response.statusCode(), response.body(), errorCode, Optional.of(containsMessage));
  }

  /**
   * As {@link #assertHttpApiException(HttpResponse, ErrorCode, String)}, but asserts only the code
   * and envelope shape. Use when the message is framework-generated and not part of the contract
   * (e.g. Armeria's own body-binding errors), so pinning its wording would be brittle.
   */
  public static void assertHttpApiException(HttpResponse<String> response, ErrorCode errorCode) {
    assertUcErrorEnvelope(response.statusCode(), response.body(), errorCode, Optional.empty());
  }

  /**
   * Sends a body-less POST to the given path. The SDK always attaches a serialized body, so raw
   * HTTP is the only way to reach the body-less code path -- where the body cannot bind and the
   * request fails as a 400 during binding, before authorization runs.
   */
  public static HttpResponse<String> sendRawEmptyPost(ServerConfig config, String path)
      throws Exception {
    return sendRawRequest(config, "POST", path, HttpRequest.BodyPublishers.noBody(), null);
  }

  /**
   * Sends a raw POST with the given JSON body and {@code Content-Type}. Use to exercise body and
   * content-type shapes the generated SDK can't produce (e.g. a trailing newline, or a
   * charset-qualified content-type) against authorization paths that read the request body.
   */
  public static HttpResponse<String> sendRawJsonPost(
      ServerConfig config, String path, String body, String contentType) throws Exception {
    return sendRawRequest(
        config, "POST", path, HttpRequest.BodyPublishers.ofString(body), contentType);
  }

  /**
   * Sends a raw POST whose body arrives as two {@code Transfer-Encoding: chunked} chunks, split at
   * the halfway byte so the break falls mid-token. Authorization reads the reassembled body, so
   * this must behave exactly like the equivalent fixed-length request.
   */
  public static HttpResponse<String> sendTwoChunkJsonPost(
      ServerConfig config, String path, String body, String contentType) throws Exception {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    int mid = bytes.length / 2;
    HttpRequest.BodyPublisher twoChunks =
        HttpRequest.BodyPublishers.fromPublisher(
            new BufferSequencePublisher(
                ByteBuffer.wrap(Arrays.copyOfRange(bytes, 0, mid)),
                ByteBuffer.wrap(Arrays.copyOfRange(bytes, mid, bytes.length))));
    return sendRawRequest(config, "POST", path, twoChunks, contentType);
  }

  /**
   * Publishes a fixed sequence of buffers, one per unit of requested demand, reporting unknown
   * length so {@code HttpRequest.BodyPublishers.fromPublisher} chunks the request.
   *
   * <p>Both properties are needed and neither comes for free. A publisher of known length (e.g.
   * {@code concat}) sends {@code Content-Length} and is not chunked at all; one of unknown length
   * but a single buffer (e.g. {@code ofInputStream}) is chunked but emits the body as one chunk.
   * Honouring demand rather than pushing every buffer at once matters too: the JDK client corrupts
   * the body if a publisher emits more than it asked for.
   */
  private record BufferSequencePublisher(ByteBuffer... buffers)
      implements Flow.Publisher<ByteBuffer> {

    @Override
    public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
      subscriber.onSubscribe(
          new Flow.Subscription() {
            private int next = 0;
            private boolean completed = false;

            @Override
            public void request(long n) {
              while (n-- > 0 && next < buffers.length) {
                subscriber.onNext(buffers[next++]);
              }
              if (next == buffers.length && !completed) {
                completed = true;
                subscriber.onComplete();
              }
            }

            @Override
            public void cancel() {}
          });
    }
  }

  /**
   * Core raw-HTTP sender shared by every raw test helper below: builds a request to {@code
   * config.getServerUrl() + path} with the given method and body, attaching the bearer token when
   * one is configured and {@code Content-Type} when non-null, and sending on a fresh client.
   */
  private static HttpResponse<String> sendRawRequest(
      ServerConfig config,
      String method,
      String path,
      HttpRequest.BodyPublisher body,
      String contentType)
      throws Exception {
    HttpRequest.Builder reqBuilder =
        HttpRequest.newBuilder().uri(URI.create(config.getServerUrl() + path)).method(method, body);
    if (contentType != null) {
      reqBuilder.header("Content-Type", contentType);
    }
    if (config.getAuthToken() != null && !config.getAuthToken().isEmpty()) {
      reqBuilder.header("Authorization", "Bearer " + config.getAuthToken());
    }
    return HttpClient.newHttpClient()
        .send(reqBuilder.build(), HttpResponse.BodyHandlers.ofString());
  }

  /**
   * Raw HTTP request with an explicit method and optional JSON body, bypassing the generated SDK,
   * for cross-channel authorization probes. Pass {@link Optional#empty()} for a body-less request;
   * otherwise the body is sent with {@code Content-Type: application/json}. The {@code path} may
   * include a query string. Used to place a parameter on the "wrong" channel (URL path, URL query,
   * or request body) relative to where the endpoint's authorization decorator reads it.
   */
  public static HttpResponse<String> sendRaw(
      ServerConfig config, String method, String path, Optional<String> jsonBody) throws Exception {
    HttpRequest.BodyPublisher body =
        jsonBody
            .map(HttpRequest.BodyPublishers::ofString)
            .orElseGet(HttpRequest.BodyPublishers::noBody);
    return sendRawRequest(
        config, method, path, body, jsonBody.isPresent() ? "application/json" : null);
  }

  /** Convenience wrapper over {@link #sendRaw} for GET probes. */
  public static HttpResponse<String> sendRawGet(
      ServerConfig config, String path, Optional<String> jsonBody) throws Exception {
    return sendRaw(config, "GET", path, jsonBody);
  }
}
