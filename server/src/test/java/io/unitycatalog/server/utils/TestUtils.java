package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.model.ErrorResponse;
import io.unitycatalog.client.delta.model.ErrorType;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.exception.ErrorCode;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
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

  public static void assertApiException(
      Executable executable, ErrorCode errorCode, String containsMessage) {
    ApiException ex = assertThrows(ApiException.class, executable);
    // Check the message first. When tests fail due to mismatching error, the message can tell us
    // more.
    assertThat(ex.getMessage()).contains(containsMessage);
    assertThat(ex.getCode()).isEqualTo(errorCode.getHttpStatus().code());
  }

  private static final ObjectMapper DELTA_ERROR_MAPPER = new ObjectMapper();

  public static void assertDeltaApiException(
      Executable executable, ErrorType expectedType, String expectedMessageSubstring) {
    int expectedCode = ErrorCode.getDeltaHttpStatus(expectedType.getValue()).code();
    ApiException ex = assertThrows(ApiException.class, executable);
    // Check message first for better diagnostics on failure (includes the full response body)
    assertThat(ex.getMessage()).contains(expectedMessageSubstring);
    assertThat(ex.getCode()).isEqualTo(expectedCode);
    try {
      ErrorResponse errorResponse =
          DELTA_ERROR_MAPPER.readValue(ex.getResponseBody(), ErrorResponse.class);
      assertThat(errorResponse.getError().getCode()).isEqualTo(expectedCode);
      assertThat(errorResponse.getError().getType()).isEqualTo(expectedType);
      assertThat(errorResponse.getError().getMessage()).contains(expectedMessageSubstring);
    } catch (Exception e) {
      assertThat(false)
          .as("Failed to parse Delta error response: " + ex.getResponseBody())
          .isTrue();
    }
  }

  /**
   * Asserts the call fails with PERMISSION_DENIED (HTTP 403) and the exception message contains
   * {@code containsMessage}. Use this when the test cares that a specific authz check fired -- e.g.
   * a staging-table ownership check -- rather than just that something produced a 403.
   */
  public static void assertPermissionDenied(Executable executable, String containsMessage) {
    assertApiException(executable, ErrorCode.PERMISSION_DENIED, containsMessage);
  }

  /**
   * Asserts the call fails with PERMISSION_DENIED (HTTP 403). Only checks the error code and the
   * generic {@code "PERMISSION_DENIED"} marker in the message; use {@link
   * #assertPermissionDenied(Executable, String)} when the specific cause matters.
   */
  public static void assertPermissionDenied(Executable executable) {
    assertPermissionDenied(executable, "PERMISSION_DENIED");
  }
}
