package io.unitycatalog.hadoop.internal.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.id.IcebergPlanCredId;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/** Fetches and converts standard Iceberg REST scan-plan credentials. */
final class IcebergPlanCredentialFetcher implements GenericCredentialFetcher {
  private static final String STORAGE_CREDENTIALS = "storage-credentials";
  private static final String PREFIX = "prefix";
  private static final String CONFIG = "config";

  private static final String S3_ACCESS_KEY_ID = "s3.access-key-id";
  private static final String S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
  private static final String S3_SESSION_TOKEN = "s3.session-token";
  private static final String S3_SESSION_TOKEN_EXPIRES_AT_MS = "s3.session-token-expires-at-ms";

  private static final String ADLS_SAS_TOKEN_PREFIX = "adls.sas-token.";
  private static final String ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX = "adls.sas-token-expires-at-ms.";

  private static final String GCS_OAUTH2_TOKEN = "gcs.oauth2.token";
  private static final String GCS_OAUTH2_TOKEN_EXPIRES_AT = "gcs.oauth2.token-expires-at";

  private final IcebergPlanCredId credId;
  private final ApiClient apiClient;

  IcebergPlanCredentialFetcher(IcebergPlanCredId credId, ApiClient apiClient) {
    Preconditions.checkNotNull(credId, "credId is required");
    Preconditions.checkNotNull(apiClient, "apiClient is required");
    this.credId = credId;
    this.apiClient = apiClient;
  }

  @Override
  public List<GenericCredential> createCredentials() throws ApiException {
    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder(requestUri()).header("Accept", "application/json").GET();
    if (apiClient.getReadTimeout() != null) {
      requestBuilder.timeout(apiClient.getReadTimeout());
    }
    Consumer<HttpRequest.Builder> requestInterceptor = apiClient.getRequestInterceptor();
    if (requestInterceptor != null) {
      requestInterceptor.accept(requestBuilder);
    }

    HttpResponse<String> response;
    try {
      response =
          apiClient
              .getHttpClient()
              .send(
                  requestBuilder.build(),
                  HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ApiException(e);
    } catch (IOException e) {
      throw new ApiException(e);
    }

    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new ApiException(response.statusCode(), response.headers(), response.body());
    }

    return parseResponse(
        apiClient.getObjectMapper(), response.body(), response.statusCode(), response.headers());
  }

  private URI requestUri() {
    String endpoint = credId.credentialsEndpoint();
    Preconditions.checkArgument(
        !endpoint.contains("#"), "credentialsEndpoint cannot contain a fragment");
    String separator = endpoint.contains("?") ? "&" : "?";
    URI uri = URI.create(endpoint + separator + "planId=" + ApiClient.urlEncode(credId.planId()));
    Preconditions.checkArgument(uri.isAbsolute(), "credentialsEndpoint must be an absolute URI");
    return uri;
  }

  private static List<GenericCredential> parseResponse(
      ObjectMapper mapper,
      String responseBody,
      int responseCode,
      java.net.http.HttpHeaders responseHeaders)
      throws ApiException {
    JsonNode root;
    try {
      root = mapper.readTree(responseBody);
    } catch (IOException e) {
      throw new ApiException(
          "Failed to parse Iceberg plan credentials response",
          e,
          responseCode,
          responseHeaders,
          responseBody);
    }

    JsonNode credentials = root == null ? null : root.get(STORAGE_CREDENTIALS);
    Preconditions.checkArgument(
        credentials != null && credentials.isArray() && !credentials.isEmpty(),
        "Iceberg credentials response contained no storage credentials");

    List<GenericCredential> result = new ArrayList<>();
    for (JsonNode credential : credentials) {
      result.add(toGenericCredential(credential));
    }
    return result;
  }

  private static GenericCredential toGenericCredential(JsonNode credential) {
    Preconditions.checkArgument(
        credential != null && credential.isObject(), "Invalid Iceberg storage credential");
    String prefix = requiredText(credential, PREFIX, "storage credential");
    JsonNode configNode = credential.get(CONFIG);
    Preconditions.checkArgument(
        configNode != null && configNode.isObject(),
        "Iceberg credential for '%s' is missing config",
        prefix);
    Map<String, String> config = stringMap(configNode, prefix);

    boolean isS3 =
        config.containsKey(S3_ACCESS_KEY_ID)
            || config.containsKey(S3_SECRET_ACCESS_KEY)
            || config.containsKey(S3_SESSION_TOKEN)
            || config.containsKey(S3_SESSION_TOKEN_EXPIRES_AT_MS);
    List<String> adlsAccounts = adlsAccounts(config);
    boolean isAzure = !adlsAccounts.isEmpty();
    boolean isGcs =
        config.containsKey(GCS_OAUTH2_TOKEN) || config.containsKey(GCS_OAUTH2_TOKEN_EXPIRES_AT);
    int cloudCount = (isS3 ? 1 : 0) + (isAzure ? 1 : 0) + (isGcs ? 1 : 0);
    Preconditions.checkArgument(
        cloudCount == 1,
        "Iceberg credential for '%s' must contain exactly one cloud credential config",
        prefix);

    if (isS3) {
      return new AwsCredential(
          requiredConfig(config, S3_ACCESS_KEY_ID, prefix),
          requiredConfig(config, S3_SECRET_ACCESS_KEY, prefix),
          requiredConfig(config, S3_SESSION_TOKEN, prefix),
          requiredExpiration(config, S3_SESSION_TOKEN_EXPIRES_AT_MS, prefix),
          prefix);
    } else if (isAzure) {
      Preconditions.checkArgument(
          adlsAccounts.size() == 1,
          "Iceberg credential for '%s' contains SAS tokens for multiple ADLS accounts",
          prefix);
      String account = adlsAccounts.get(0);
      return new AzureCredential(
          requiredConfig(config, ADLS_SAS_TOKEN_PREFIX + account, prefix),
          requiredExpiration(config, ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + account, prefix),
          prefix);
    } else {
      return new GcsCredential(
          requiredConfig(config, GCS_OAUTH2_TOKEN, prefix),
          requiredExpiration(config, GCS_OAUTH2_TOKEN_EXPIRES_AT, prefix),
          prefix);
    }
  }

  private static Map<String, String> stringMap(JsonNode config, String prefix) {
    Map<String, String> result = new LinkedHashMap<>();
    Iterator<Map.Entry<String, JsonNode>> fields = config.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      Preconditions.checkArgument(
          field.getValue().isTextual(),
          "Iceberg credential config '%s' for '%s' must be a string",
          field.getKey(),
          prefix);
      result.put(field.getKey(), field.getValue().textValue());
    }
    return result;
  }

  private static List<String> adlsAccounts(Map<String, String> config) {
    List<String> accounts = new ArrayList<>();
    for (String key : config.keySet()) {
      if (key.startsWith(ADLS_SAS_TOKEN_PREFIX) && key.length() > ADLS_SAS_TOKEN_PREFIX.length()) {
        accounts.add(key.substring(ADLS_SAS_TOKEN_PREFIX.length()));
      }
    }
    return accounts;
  }

  private static String requiredText(JsonNode node, String key, String description) {
    JsonNode value = node.get(key);
    Preconditions.checkArgument(
        value != null && value.isTextual() && !value.textValue().isEmpty(),
        "Iceberg %s is missing %s",
        description,
        key);
    return value.textValue();
  }

  private static String requiredConfig(Map<String, String> config, String key, String prefix) {
    String value = config.get(key);
    Preconditions.checkArgument(
        value != null && !value.isEmpty(),
        "Iceberg credential for '%s' is missing config '%s'",
        prefix,
        key);
    return value;
  }

  private static long requiredExpiration(Map<String, String> config, String key, String prefix) {
    String value = requiredConfig(config, key, prefix);
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format(
              "Iceberg credential config '%s' for '%s' must be epoch milliseconds", key, prefix),
          e);
    }
  }
}
