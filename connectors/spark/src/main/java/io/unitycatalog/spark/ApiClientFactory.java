package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.ApiClientUtils;
import io.unitycatalog.client.retry.RetryPolicy;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class ApiClientFactory {

  private ApiClientFactory() {}

  public static ApiClient createApiClient(
      RetryPolicy retryPolicy, URI uri, TokenProvider tokenProvider) {
    return ApiClientUtils.create(uri, tokenProvider, retryPolicy, appEngineVersions());
  }

  static Map<String, String> appEngineVersions() {
    Map<String, String> versions = new LinkedHashMap<>();
    putIfNotNull(versions, "Spark", getSparkVersion());
    putIfNotNull(versions, "Delta", DeltaVersionUtils.getDeltaVersion());
    putIfNotNull(versions, "Java", getJavaVersion());
    putIfNotNull(versions, "Scala", getScalaVersion());
    return Collections.unmodifiableMap(versions);
  }

  private static void putIfNotNull(Map<String, String> versions, String name, String version) {
    if (version != null) {
      versions.put(name, version);
    }
  }

  private static String getSparkVersion() {
    try {
      return org.apache.spark.package$.MODULE$.SPARK_VERSION();
    } catch (Exception e) {
      return null;
    }
  }

  private static String getJavaVersion() {
    try {
      return System.getProperty("java.version");
    } catch (Exception e) {
      return null;
    }
  }

  private static String getScalaVersion() {
    try {
      return scala.util.Properties.versionNumberString();
    } catch (Exception e) {
      // Scala not available or version not accessible
      return null;
    }
  }
}
