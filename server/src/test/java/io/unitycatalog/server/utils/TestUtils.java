package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.DeltaApiException;
import io.unitycatalog.client.delta.model.DeltaErrorType;
import io.unitycatalog.client.internal.ApiClientUtils;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.exception.ErrorCode;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.function.Executable;

public final class TestUtils {

  private TestUtils() {}

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

  public static final Map<String, String> PROPERTIES = Map.of("prop1", "value1", "prop2", "value2");
  public static final Map<String, String> NEW_PROPERTIES =
      Map.of("prop2", "value22", "prop3", "value33");
  public static final String COMMON_ENTITY_NAME = "zz_uc_common_entity_name";

  public static ApiClient createApiClient(ServerConfig serverConfig) {
    URI uri = URI.create(serverConfig.getServerUrl());
    String token = serverConfig.getAuthToken() != null ? serverConfig.getAuthToken() : "";
    return ApiClientUtils.create(
        uri,
        TokenProvider.create(Map.of("type", "static", "token", token)),
        JitterDelayRetryPolicy.builder().maxAttempts(1).build(),
        Collections.emptyMap());
  }

  public static void assertApiException(
      Executable executable, ErrorCode errorCode, String containsMessage) {
    ApiException ex = assertThrows(ApiException.class, executable);
    // Check the message first. When tests fail due to mismatching error, the message can tell us
    // more.
    assertThat(ex.getMessage()).contains(containsMessage);
    assertThat(ex.getCode()).isEqualTo(errorCode.getHttpStatus().code());
  }

  /**
   * Asserts the call fails by checking the HTTP status code is {@code expectedStatus}. Use for
   * body-less responses; otherwise use {@link #assertApiException} / {@link
   * #assertDeltaApiException}.
   */
  public static void assertApiExceptionStatusOnly(Executable executable, int expectedStatus) {
    ApiException ex = assertThrows(ApiException.class, executable);
    assertThat(ex.getCode()).isEqualTo(expectedStatus);
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
