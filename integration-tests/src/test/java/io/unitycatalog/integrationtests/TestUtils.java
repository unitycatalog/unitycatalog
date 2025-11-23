package io.unitycatalog.integrationtests;

import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;

public class TestUtils {
  public static final String CATALOG_NAME = envAsString("CATALOG_NAME", "unity");
  public static final String SCHEMA_NAME = envAsString("SCHEMA_NAME", "UcIntegrationTest");
  public static final String SERVER_URL = envAsString("CATALOG_URI", "http://localhost:8080");

  // Fixed token to authenticate unity catalog server.
  public static final String AUTH_TOKEN = envAsString("CATALOG_AUTH_TOKEN", "");

  // OAuth to authenticate unity catalog server.
  public static final String OAUTH_URI = envAsString("CATALOG_OAUTH_URI", "");
  public static final String OAUTH_CLIENT_ID = envAsString("CATALOG_OAUTH_CLIENT_ID", "");
  public static final String OAUTH_CLIENT_SECRET = envAsString("CATALOG_OAUTH_CLIENT_SECRET", "");

  // Base locations.
  public static final String S3_BASE_LOCATION = envAsString("S3_BASE_LOCATION", "");
  public static final String GS_BASE_LOCATION = envAsString("GS_BASE_LOCATION", "");
  public static final String ABFSS_BASE_LOCATION = envAsString("ABFSS_BASE_LOCATION", "");
  public static final List<String> BASE_LOCATIONS =
      List.of(S3_BASE_LOCATION, GS_BASE_LOCATION, ABFSS_BASE_LOCATION);

  public static String envAsString(String key, String defaultValue) {
    return System.getenv().getOrDefault(key, defaultValue);
  }

  public static long envAsLong(String key, long defaultValue) {
    String value = System.getenv().get(key);
    return value == null ? defaultValue : Long.parseLong(value);
  }

  public static boolean envAsBoolean(String key, boolean defaultValue) {
    String value = System.getenv().get(key);
    return value == null ? defaultValue : Boolean.parseBoolean(value);
  }

  public static String randomName() {
    return RandomStringUtils.secure().nextAlphabetic(8);
  }

  public static boolean isEmptyOrNull(String value) {
    return value == null || value.isEmpty();
  }
}
