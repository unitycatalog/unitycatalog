package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.common.utils.VersionUtils;
import io.unitycatalog.spark.utils.Clock;
import java.net.URI;
import java.net.http.HttpRequest;
import java.util.Map;
import java.util.function.Consumer;

public class ApiClientFactory {
  public static final Map<String, String> VERSIONS = Map.of(
      "SparkVersion", sparkVersion(),
      "DeltaVersion", deltaVersion()
  );

  public static final String UNKNOWN_VERSION = "unknown";

  public static final String BASE_PATH = "/api/2.1/unity-catalog";

  private ApiClientFactory() {
  }

  public static ApiClient createApiClient(ApiClientConf clientConf, URI url, String token) {
    return createApiClient(clientConf, url, token, VERSIONS);
  }

  public static ApiClient createApiClient(
      ApiClientConf clientConf, URI url, String token, Map<String, String> userAgentInfo) {
    // Base path in ApiClient is already set to `BASE_PATH`, so we override it to provide
    // base path from given `url` but still preserving path suffix.
    // Expected input for `url` is URL with no "/api/2.1/unity-catalog" in the path.
    String basePath = url.getPath() + BASE_PATH;
    RetryingApiClient apiClient = new RetryingApiClient(clientConf, Clock.systemClock());
    apiClient.setHost(url.getHost())
        .setPort(url.getPort())
        .setScheme(url.getScheme())
        .setBasePath(basePath)
        .setRequestInterceptor(new UCHttpInterceptor(token, userAgentInfo));

    return apiClient;
  }

  public static class UCHttpInterceptor implements Consumer<HttpRequest.Builder> {
    private final String token;
    private final Map<String, String> userAgentMap;

    public UCHttpInterceptor(String token, Map<String, String> userAgentMap) {
      this.token = token;
      this.userAgentMap = userAgentMap;
    }

    @Override
    public void accept(HttpRequest.Builder builder) {
      // Add the Authorization if token is available.
      if (token != null && !token.isEmpty()) {
        builder.header("Authorization", "Bearer " + token);
      }

      // Add the User-Agent to indicate the components versions.

      // Add the unitycatalog sdk version by default.
      StringBuilder sb = new StringBuilder();
      sb.append("UnityCatalog-Java-Client").append("/").append(VersionUtils.VERSION);

      // Add the extra user agent info.
      for (Map.Entry<String, String> entry : userAgentMap.entrySet()) {
        if (sb.length() > 0) {
          sb.append(" ");
        }
        sb.append(entry.getKey()).append("/").append(entry.getValue());
      }
      builder.header("User-Agent", sb.toString());
    }
  }

  private static String sparkVersion() {
    try {
      return org.apache.spark.package$.MODULE$.SPARK_VERSION();
    } catch (Exception e) {
      return UNKNOWN_VERSION;
    }
  }

  private static String deltaVersion() {
    try {
      Class<?> versionClass = Class.forName("io.delta.Version");
      Object versionObj = versionClass.getMethod("getVersion").invoke(null);
      return versionObj != null ? versionObj.toString() : UNKNOWN_VERSION;
    } catch (Exception e) {
      // Fall back to io.delta.VERSION constant (older versions)
      try {
        Class<?> packageClass = Class.forName("io.delta.package$");
        Object versionObj = packageClass.getMethod("VERSION").invoke(
            packageClass.getField("MODULE$").get(null));
        return versionObj != null ? versionObj.toString() : UNKNOWN_VERSION;
      } catch (Exception e2) {
        // Delta not available or version not accessible
        return UNKNOWN_VERSION;
      }
    }
  }
}
