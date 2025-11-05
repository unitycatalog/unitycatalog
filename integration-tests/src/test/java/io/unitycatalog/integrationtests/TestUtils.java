package io.unitycatalog.integrationtests;

import org.apache.commons.lang3.RandomStringUtils;

public class TestUtils {
  public static final String SERVER_URL = envAsString("CATALOG_URI", "http://localhost:8080");
  public static final String AUTH_TOKEN = envAsString("CATALOG_AUTH_TOKEN", "");
  public static final String CATALOG_NAME = envAsString("CATALOG_NAME", "unity");
  public static final String SCHEMA_NAME = envAsString("SCHEMA_NAME", randomName());
  public static final String S3_BASE_LOCATION = envAsString("S3_BASE_LOCATION", "s3://bucket/key");
  public static final String GS_BASE_LOCATION = envAsString("GS_BASE_LOCATION", "gs://bucket/key");
  public static final String ABFSS_BASE_LOCATION = envAsString("ABFSS_BASE_LOCATION",
      "abfss://bucket/key");

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
}
