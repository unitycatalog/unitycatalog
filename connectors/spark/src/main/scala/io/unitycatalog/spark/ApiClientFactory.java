package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.auth.UCTokenProvider;
import io.unitycatalog.client.retry.RetryPolicy;
import java.net.URI;

public class ApiClientFactory {

  private ApiClientFactory() {
  }

  public static ApiClient createApiClient(
      RetryPolicy retryPolicy, URI url, UCTokenProvider ucTokenProvider) {

    // Create a new ApiClient Builder.
    ApiClientBuilder builder = ApiClientBuilder.create()
        .url(url)
        .ucTokenProvider(ucTokenProvider)
        .retryPolicy(retryPolicy);

    // Add Spark to User-Agent, and Delta if available
    String sparkVersion = getSparkVersion();
    String deltaVersion = getDeltaVersion();
    if (deltaVersion != null) {
      builder.clientVersion("Spark", sparkVersion, "Delta", deltaVersion);
    } else {
      builder.clientVersion("Spark", sparkVersion);
    }

    return builder.build();
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
