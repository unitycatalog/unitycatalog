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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.auth.AuthorizationRequest.Principal;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class OpaAuthorizationProviderTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String POLICY_PATH = "/v1/data/unitycatalog/authorize";
  private static final String BEARER_TOKEN = "sensitive-opa-token";
  private static final String FAILURE_MESSAGE = "Authorization policy evaluation failed.";
  private static final Duration DEFAULT_TEST_TIMEOUT = Duration.ofSeconds(2);

  private static final UUID PRINCIPAL_ID = UUID.fromString("10000000-0000-0000-0000-000000000001");
  private static final UUID METASTORE_ID = UUID.fromString("20000000-0000-0000-0000-000000000002");
  private static final UUID EXTERNAL_LOCATION_ID =
      UUID.fromString("30000000-0000-0000-0000-000000000003");
  private static final UUID CREDENTIAL_ID = UUID.fromString("40000000-0000-0000-0000-000000000004");

  private static final AuthorizationRequest REQUEST =
      new AuthorizationRequest(
          new Principal(PRINCIPAL_ID, "alice@example.com"),
          AuthorizationAction.GENERATE_PATH_CREDENTIALS,
          Map.of(
              SecurableType.METASTORE,
              METASTORE_ID,
              SecurableType.EXTERNAL_LOCATION,
              EXTERNAL_LOCATION_ID,
              SecurableType.CREDENTIAL,
              CREDENTIAL_ID),
          Map.of(
              SecurableType.EXTERNAL_LOCATION,
              "s3://customer-data/private/",
              SecurableType.CREDENTIAL,
              "production-credential"),
          Map.of("credential_scope", "READ_WRITE", "skip_validation", false, "page_size", 25));

  private static final AtomicReference<AggregatedHttpRequest> RECEIVED_REQUEST =
      new AtomicReference<>();
  private static final AtomicReference<StubResponse> STUB_RESPONSE = new AtomicReference<>();

  private static Server server;
  private static String policyUrl;

  @BeforeAll
  static void startServer() {
    server =
        Server.builder().http(0).service(POLICY_PATH, OpaAuthorizationProviderTest::serve).build();
    server.start().join();
    policyUrl = "http://127.0.0.1:" + server.activeLocalPort() + POLICY_PATH + "?policy_revision=7";
  }

  @AfterAll
  static void stopServer() {
    server.stop().join();
  }

  @BeforeEach
  void resetServer() {
    RECEIVED_REQUEST.set(null);
    STUB_RESPONSE.set(new StubResponse(HttpStatus.OK, "{\"result\":true}", Duration.ZERO));
  }

  @Test
  void postsSerializedInputToConfiguredUrlWithBearerToken() throws Exception {
    OpaAuthorizationProvider provider = provider(DEFAULT_TEST_TIMEOUT, BEARER_TOKEN);

    assertThat(provider.authorize(REQUEST)).isTrue();

    AggregatedHttpRequest request = RECEIVED_REQUEST.get();
    assertThat(request).isNotNull();
    assertThat(request.method()).isEqualTo(HttpMethod.POST);
    assertThat(request.path()).isEqualTo(POLICY_PATH + "?policy_revision=7");
    assertThat(request.headers().contentType()).isEqualTo(MediaType.JSON);
    assertThat(request.headers().get(HttpHeaderNames.AUTHORIZATION))
        .isEqualTo("Bearer " + BEARER_TOKEN);
    assertThat(MAPPER.readTree(request.contentUtf8()))
        .isEqualTo(
            MAPPER.readTree(
                """
                {
                  "input": {
                    "principal": {
                      "id": "10000000-0000-0000-0000-000000000001",
                      "email": "alice@example.com"
                    },
                    "action": "GENERATE_PATH_CREDENTIALS",
                    "resources": {
                      "metastore": "20000000-0000-0000-0000-000000000002",
                      "external_location": "30000000-0000-0000-0000-000000000003",
                      "credential": "40000000-0000-0000-0000-000000000004"
                    },
                    "resource_keys": {
                      "external_location": "s3://customer-data/private/",
                      "credential": "production-credential"
                    },
                    "context": {
                      "credential_scope": "READ_WRITE",
                      "skip_validation": false,
                      "page_size": 25
                    }
                  }
                }
                """));
  }

  @Test
  void returnsExplicitFalseWithoutAddingAuthorizationHeader() {
    STUB_RESPONSE.set(new StubResponse(HttpStatus.OK, "{\"result\":false}", Duration.ZERO));
    OpaAuthorizationProvider provider = provider(DEFAULT_TEST_TIMEOUT, null);

    assertThat(provider.authorize(REQUEST)).isFalse();
    assertThat(RECEIVED_REQUEST.get().headers().get(HttpHeaderNames.AUTHORIZATION)).isNull();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidResponses")
  void rejectsInvalidResponses(
      String description, HttpStatus status, String responseBody, String sensitiveValue) {
    STUB_RESPONSE.set(new StubResponse(status, responseBody, Duration.ZERO));

    assertPolicyFailure(provider(DEFAULT_TEST_TIMEOUT, BEARER_TOKEN), sensitiveValue, BEARER_TOKEN);
  }

  @Test
  void rejectsResponseTimeoutWithoutAttachingCause() {
    STUB_RESPONSE.set(new StubResponse(HttpStatus.OK, "{\"result\":true}", Duration.ofMillis(500)));

    assertPolicyFailure(provider(Duration.ofMillis(50), BEARER_TOKEN), BEARER_TOKEN);
    assertThat(RECEIVED_REQUEST.get()).isNotNull();
  }

  @Test
  void rejectsResponseLargerThanDefaultLimit() {
    String sensitiveValue = "oversized-upstream-secret";
    String responseBody =
        "{\"result\":true,\"detail\":\""
            + sensitiveValue
            + "\",\"padding\":\""
            + "x".repeat(64 * 1024)
            + "\"}";
    STUB_RESPONSE.set(new StubResponse(HttpStatus.OK, responseBody, Duration.ZERO));

    assertPolicyFailure(provider(DEFAULT_TEST_TIMEOUT, BEARER_TOKEN), sensitiveValue, BEARER_TOKEN);
  }

  private static Stream<Arguments> invalidResponses() {
    return Stream.of(
        Arguments.of(
            "missing result",
            HttpStatus.OK,
            "{\"detail\":\"missing-result-secret\"}",
            "missing-result-secret"),
        Arguments.of(
            "null result",
            HttpStatus.OK,
            "{\"result\":null,\"detail\":\"null-result-secret\"}",
            "null-result-secret"),
        Arguments.of(
            "string result",
            HttpStatus.OK,
            "{\"result\":\"true\",\"detail\":\"string-result-secret\"}",
            "string-result-secret"),
        Arguments.of(
            "array result",
            HttpStatus.OK,
            "{\"result\":[true],\"detail\":\"array-result-secret\"}",
            "array-result-secret"),
        Arguments.of(
            "malformed response",
            HttpStatus.OK,
            "{\"result\":true,\"detail\":\"malformed-response-secret\"",
            "malformed-response-secret"),
        Arguments.of(
            "trailing content",
            HttpStatus.OK,
            "{\"result\":true} trailing-response-secret",
            "trailing-response-secret"),
        Arguments.of(
            "multiple decisions",
            HttpStatus.OK,
            "{\"result\":true}{\"result\":false,\"detail\":\"second-response-secret\"}",
            "second-response-secret"),
        Arguments.of(
            "non-200 response",
            HttpStatus.CREATED,
            "{\"result\":true,\"detail\":\"non-200-secret\"}",
            "non-200-secret"));
  }

  private static OpaAuthorizationProvider provider(Duration timeout, String bearerToken) {
    Properties properties = new Properties();
    properties.setProperty(Property.AUTHORIZATION_ENABLED.getKey(), "enable");
    properties.setProperty(Property.AUTHORIZATION_PROVIDER.getKey(), "opa");
    properties.setProperty(Property.OPA_POLICY_URL.getKey(), policyUrl);
    properties.setProperty(Property.OPA_RESPONSE_TIMEOUT.getKey(), timeout.toString());
    if (bearerToken != null) {
      properties.setProperty(Property.OPA_BEARER_TOKEN.getKey(), bearerToken);
    }
    return new OpaAuthorizationProvider(new ServerProperties(properties));
  }

  private static void assertPolicyFailure(
      OpaAuthorizationProvider provider, String... sensitiveValues) {
    BaseException error = assertThrows(BaseException.class, () -> provider.authorize(REQUEST));
    assertThat(error.getErrorCode()).isEqualTo(ErrorCode.INTERNAL);
    assertThat(error.getErrorMessage()).isEqualTo(FAILURE_MESSAGE);
    assertThat(error).hasMessage(FAILURE_MESSAGE).hasNoCause();
    for (String sensitiveValue : sensitiveValues) {
      assertThat(error.toString()).doesNotContain(sensitiveValue);
    }
  }

  private static HttpResponse serve(ServiceRequestContext context, HttpRequest request) {
    return HttpResponse.of(
        request
            .aggregate()
            .thenApply(
                aggregated -> {
                  RECEIVED_REQUEST.set(aggregated);
                  StubResponse stub = STUB_RESPONSE.get();
                  HttpResponse response =
                      HttpResponse.of(stub.status(), MediaType.JSON_UTF_8, stub.body());
                  if (stub.delay().isZero()) {
                    return response;
                  }
                  return HttpResponse.delayed(response, stub.delay(), context.eventLoop());
                }));
  }

  private record StubResponse(HttpStatus status, String body, Duration delay) {}
}
