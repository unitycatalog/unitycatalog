package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import org.apache.spark.SparkContext;

import java.net.URI;

public class ApiClientFactory {
  private ApiClientFactory() {}

  public static ApiClient createApiClient(URI url, String token) {
    ApiClient apiClient = new ApiClient()
        .setHost(url.getHost())
        .setPort(url.getPort())
        .setScheme(url.getScheme());

    // Add Spark and Delta versions to User-Agent
    try {
      String sparkVersion = getSparkVersion();
      String deltaVersion = getDeltaVersion();
      
      if (deltaVersion != null) {
        apiClient.setClientVersion("Spark", sparkVersion, "Delta", deltaVersion);
      } else {
        apiClient.setClientVersion("Spark", sparkVersion);
      }
    } catch (Exception e) {
      // If we can't get versions, continue without them
      // This shouldn't fail the client creation
    }

    if (token != null && !token.isEmpty()) {
      apiClient = apiClient.setRequestInterceptor(
          request -> request.header("Authorization", "Bearer " + token)
      );
    }

    return apiClient;
  }

  private static String getSparkVersion() {
    try {
      return SparkContext.version();
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
        Object versionObj = packageClass.getMethod("VERSION").invoke(packageClass.getField("MODULE$").get(null));
        return versionObj != null ? versionObj.toString() : null;
      } catch (Exception e2) {
        // Delta not available or version not accessible
        return null;
      }
    }
  }
}
