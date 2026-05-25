package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.auth.AuthToken;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that verify the error response format, particularly that stack traces are excluded by
 * default.
 */
public class ErrorResponseTest extends BaseServerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private WebClient client;

  @Override
  protected void setUpProperties() {
    serverProperties = new Properties();
    serverProperties.setProperty(Property.SERVER_ENV.getKey(), "test");
    // Do NOT set INCLUDE_STACK_TRACE_IN_ERROR -- it defaults to false
    serverProperties.setProperty(Property.MANAGED_TABLE_ENABLED.getKey(), "true");
    tableStorageRoot = getManagedStorageCloudPath(testDirectoryRoot);
    serverProperties.setProperty(Property.TABLE_STORAGE_ROOT.getKey(), tableStorageRoot);
  }

  @BeforeEach
  public void setUp() {
    super.setUp();
    String uri = serverConfig.getServerUrl() + "/api/2.1/unity-catalog";
    String token = serverConfig.getAuthToken();
    client = WebClient.builder(uri).auth(AuthToken.ofOAuth2(token)).build();
  }

  @Test
  public void testErrorResponseOmitsStackTraceByDefault() throws Exception {
    // Trigger a NOT_FOUND error
    AggregatedHttpResponse resp = client.get("/catalogs/nonexistent").aggregate().join();
    assertThat(resp.status()).isEqualTo(HttpStatus.NOT_FOUND);

    JsonNode json = MAPPER.readTree(resp.contentUtf8());
    assertThat(json.has("error_code")).isTrue();
    assertThat(json.get("error_code").asText()).isEqualTo("NOT_FOUND");
    assertThat(json.has("message")).isTrue();
    assertThat(json.has("stack_trace"))
        .as("stack_trace should be absent when include-stacktrace-in-error is false (default)")
        .isFalse();
  }
}
