package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.spark.auth.catalog.UCTokenProvider;
import io.unitycatalog.spark.utils.Clock;
import java.net.URI;

public class ApiClientFactory {

  public static final String BASE_PATH = "/api/2.1/unity-catalog";

  private ApiClientFactory() {
  }

  public static ApiClient createApiClient(
      ApiClientConf clientConf, URI url, UCTokenProvider ucTokenProvider) {
    // Base path in ApiClient is already set to `BASE_PATH`, so we override it to provide
    // base path from given `url` but still preserving path suffix.
    // Expected input for `url` is URL with no "/api/2.1/unity-catalog" in the path.
    String basePath = url.getPath() + BASE_PATH;
    RetryingApiClient apiClient = new RetryingApiClient(clientConf, Clock.systemClock());
    apiClient.setHost(url.getHost())
        .setPort(url.getPort())
        .setScheme(url.getScheme())
        .setBasePath(basePath);

    // Add Spark to User-Agent, and Delta if available
    String sparkVersion = getSparkVersion();
    String deltaVersion = getDeltaVersion();
    if (deltaVersion != null) {
      apiClient.setClientVersion("Spark", sparkVersion, "Delta", deltaVersion);
    } else {
      apiClient.setClientVersion("Spark", sparkVersion);
    }

    if (ucTokenProvider != null) {
      apiClient.setRequestInterceptor(
          request -> request.header("Authorization", "Bearer " + ucTokenProvider.accessToken())
      );
    }
    return apiClient;
  }

  private static String getSparkVersion() {
    try {
      return org.apache.spark.package$.MODULE$.SPARK_VERSION();
    } catch (Exception e) {
      return null;
    }
  }

  private static String getDeltaVersion() {
    // Try io.delta.Version.getVersion() first (preferred method)
    try {
      Class<?> versionClass = Class.forName("io.delta.Version");
      Object versionObj = versionClass.getMethod("getVersion").invoke(null);
      return versionObj != null ? versionObj.toString() : null;
    } catch (Exception e) {
      // Fall back to io.delta.VERSION constant (older versions)
      try {
        Class<?> packageClass = Class.forName("io.delta.package$");
        Object versionObj = packageClass.getMethod("VERSION").invoke(
            packageClass.getField("MODULE$").get(null));
        return versionObj != null ? versionObj.toString() : null;
      } catch (Exception e2) {
        // Delta not available or version not accessible
        return null;
      }
    }
  }
}
