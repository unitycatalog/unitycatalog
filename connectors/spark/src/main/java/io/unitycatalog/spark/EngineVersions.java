package io.unitycatalog.spark;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Detects the engine's Spark / Delta / Java / Scala versions, reported in the User-Agent header via
 * {@code ApiClientFactory}. Lives in the connector because it references Spark and Delta classes
 * the client module does not depend on.
 */
public final class EngineVersions {

  private EngineVersions() {}

  public static Map<String, String> appEngineVersions() {
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
