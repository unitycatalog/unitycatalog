package io.unitycatalog.integrationtests;

import org.apache.commons.lang3.RandomStringUtils;

public class TestUtils {
  public static final String SERVER_URL = envString("CATALOG_URI", "http://localhost:8080");
  public static final String AUTH_TOKEN = envString("CATALOG_AUTH_TOKEN", "");
  public static final String CATALOG_NAME = envString("CATALOG_NAME", "unity");
  public static final String SCHEMA_NAME = envString("SCHEMA_NAME", randomName());
  public static final String S3_BASE_LOCATION = envString("S3_BASE_LOCATION", "s3://bucket/key");
  public static final String GS_BASE_LOCATION = envString("GS_BASE_LOCATION", "gs://bucket/key");
  public static final String ABFSS_BASE_LOCATION = envString("ABFSS_BASE_LOCATION",
      "abfss://bucket/key");

  public static String envString(String key, String defaultValue) {
    return System.getenv().getOrDefault(key, defaultValue);
  }

  public static long envLong(String key, long defaultValue) {
    String value = System.getenv().get(key);
    return value == null ? defaultValue : Long.parseLong(value);
  }

  public static boolean envBoolean(String key, boolean defaultValue) {
    String value = System.getenv().get(key);
    return value == null ? defaultValue : Boolean.parseBoolean(value);
  }

  public static String randomName() {
    return RandomStringUtils.secure().nextAlphabetic(8);
  }
}
