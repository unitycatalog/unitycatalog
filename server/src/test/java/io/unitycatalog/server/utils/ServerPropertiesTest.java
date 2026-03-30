package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class ServerPropertiesTest {

  private void testValidProperty(Property property, String value) {
    Properties props = new Properties();
    props.setProperty(property.getKey(), value);
    ServerProperties serverProperties1 = new ServerProperties(props);
    assertThat(serverProperties1.get(property)).isEqualTo(value);

    ServerProperties serverProperties2 = new ServerProperties();
    serverProperties2.set(property, value);
    assertThat(serverProperties2.get(property)).isEqualTo(value);
  }

  private void testInvalidProperty(Property property, String value, String... exceptionMessages) {
    Properties props = new Properties();
    props.setProperty(property.getKey(), value);
    var assertion1 =
        assertThatThrownBy(() -> new ServerProperties(props)).isInstanceOf(BaseException.class);
    for (String exceptionMessage : exceptionMessages) {
      assertion1.hasMessageContaining(exceptionMessage);
    }

    ServerProperties serverProperties2 = new ServerProperties();
    var assertion2 =
        assertThatThrownBy(() -> serverProperties2.set(property, value))
            .isInstanceOf(BaseException.class);
    for (String exceptionMessage : exceptionMessages) {
      assertion2.hasMessageContaining(exceptionMessage);
    }
  }

  @Test
  public void testStoragePathValidator() {
    // Valid paths - local absolute and relative
    testValidProperty(Property.MODEL_STORAGE_ROOT, "/tmp/my-storage_123");
    testValidProperty(Property.MODEL_STORAGE_ROOT, "/tmp/storage/");
    testValidProperty(Property.MODEL_STORAGE_ROOT, "relative/path/to/storage");
    testValidProperty(Property.TABLE_STORAGE_ROOT, "./relative/path");

    // Valid paths - file URIs
    testValidProperty(Property.MODEL_STORAGE_ROOT, "file:///tmp/my-storage");
    testValidProperty(Property.TABLE_STORAGE_ROOT, "file://tmp/my-storage");
    testValidProperty(Property.TABLE_STORAGE_ROOT, "file:/tmp/my-storage");

    // Valid paths - cloud storage (S3, ABFS, GCS)
    testValidProperty(Property.MODEL_STORAGE_ROOT, "s3://my-bucket/my-prefix");
    testValidProperty(Property.TABLE_STORAGE_ROOT, "s3://my-bucket/prefix/");
    testValidProperty(
        Property.MODEL_STORAGE_ROOT, "abfs://my-container@my-storage.dfs.core.windows.net/my-path");
    testValidProperty(Property.TABLE_STORAGE_ROOT, "gs://my-bucket/my-path");

    // Invalid - unsupported URI schemes
    testInvalidProperty(
        Property.MODEL_STORAGE_ROOT,
        "ftp://example.com/path",
        "Invalid storage path",
        "storage-root.models",
        "Unsupported URI scheme: ftp");
    testInvalidProperty(
        Property.TABLE_STORAGE_ROOT,
        "http://example.com/path",
        "Invalid storage path",
        "storage-root.tables",
        "Unsupported URI scheme: http");
    testInvalidProperty(
        Property.MODEL_STORAGE_ROOT,
        "https://example.com/path",
        "Invalid storage path",
        "storage-root.models",
        "Unsupported URI scheme: https");

    // Invalid - malformed URIs
    testInvalidProperty(
        Property.MODEL_STORAGE_ROOT,
        "s3://bucket with spaces/path",
        "Invalid storage path",
        "storage-root.models");
    testInvalidProperty(
        Property.MODEL_STORAGE_ROOT,
        "C:\\Users\\test\\storage",
        "Invalid storage path",
        "storage-root.models");
  }

  @Test
  public void testBooleanValidator() {
    // Valid values
    testValidProperty(Property.MANAGED_TABLE_ENABLED, "true");
    testValidProperty(Property.MANAGED_TABLE_ENABLED, "false");
    testValidProperty(Property.MANAGED_TABLE_ENABLED, "TRUE");

    // Invalid values
    testInvalidProperty(
        Property.MANAGED_TABLE_ENABLED,
        "yes",
        "Invalid value 'yes'",
        "server.managed-table.enabled",
        "Allowed values: [true, false]");
  }

  @Test
  public void testEnumValidator() {
    // Valid values - SERVER_ENV
    testValidProperty(Property.SERVER_ENV, "dev");
    testValidProperty(Property.SERVER_ENV, "prod");
    testValidProperty(Property.SERVER_ENV, "test");
    testValidProperty(Property.SERVER_ENV, "PROD");

    // Valid values - AUTHORIZATION_ENABLED
    testValidProperty(Property.AUTHORIZATION_ENABLED, "enable");
    testValidProperty(Property.AUTHORIZATION_ENABLED, "disable");

    // Invalid values
    testInvalidProperty(Property.SERVER_ENV, "staging", "Invalid value 'staging'", "server.env");
    testInvalidProperty(
        Property.AUTHORIZATION_ENABLED, "yes", "Invalid value 'yes'", "server.authorization");
  }

  @Test
  public void testUrlValidator() {
    // Valid URLs
    testValidProperty(Property.AUTHORIZATION_URL, "https://auth.example.com/authorize");
    testValidProperty(Property.TOKEN_URL, "http://localhost:8080/token");

    // Invalid URLs
    testInvalidProperty(
        Property.AUTHORIZATION_URL,
        "not a valid url",
        "Invalid URL 'not a valid url'",
        "server.authorization-url");
    testInvalidProperty(
        Property.TOKEN_URL,
        "example.com/path",
        "Invalid URL 'example.com/path'",
        "server.token-url");
    testInvalidProperty(
        Property.TOKEN_URL,
        "s3://example.com/path",
        "Invalid URL 's3://example.com/path'",
        "server.token-url");
  }

  @Test
  public void testPositiveIntegerValidator() {
    // Valid values
    testValidProperty(Property.REDIRECT_PORT, "8080");
    testValidProperty(Property.REDIRECT_PORT, "65535");

    // Invalid values
    testInvalidProperty(
        Property.REDIRECT_PORT,
        "0",
        "Invalid value '0'",
        "server.redirect-port",
        "Expected a positive integer (> 0)");
    testInvalidProperty(
        Property.REDIRECT_PORT,
        "-1",
        "Invalid value '-1'",
        "server.redirect-port",
        "Expected a positive integer (> 0)");
    testInvalidProperty(
        Property.REDIRECT_PORT,
        "abc",
        "Invalid value 'abc'",
        "server.redirect-port",
        "Expected an integer");
    testInvalidProperty(
        Property.REDIRECT_PORT,
        "8080.5",
        "Invalid value '8080.5'",
        "server.redirect-port",
        "Expected an integer");
  }

  @Test
  public void testDurationValidator() {
    // Valid values
    testValidProperty(Property.COOKIE_TIMEOUT, "P5D");
    testValidProperty(Property.COOKIE_TIMEOUT, "PT2H");
    testValidProperty(Property.COOKIE_TIMEOUT, "P1DT12H30M");

    // Invalid values
    testInvalidProperty(
        Property.COOKIE_TIMEOUT, "5 days", "Invalid value '5 days'", "server.cookie-timeout");
    testInvalidProperty(
        Property.COOKIE_TIMEOUT, "P5X", "Invalid value 'P5X'", "server.cookie-timeout");
  }

  @Test
  public void testDefaultPropertyValues() {
    // Test that all default values are valid and correctly set. If any of them are invalid, the
    // constructor would throw exception.
    ServerProperties serverProperties = new ServerProperties();
    for (Property property : Property.values()) {
      if (property.getDefaultValue() != null) {
        assertThat(serverProperties.get(property)).isEqualTo(property.getDefaultValue());
      }
    }

    // Test that properties from etc/conf/server.properties match the default values.
    // If you have modified values in file server.properties, please also update ServerProperties.
    ServerProperties serverPropertiesFromConf = new ServerProperties("etc/conf/server.properties");
    for (Property property : Property.values()) {
      // Compare values - both should have the same defaults
      assertThat(serverPropertiesFromConf.get(property))
          .as("Property %s from etc/conf/server.properties should match default", property.getKey())
          .isEqualTo(serverProperties.get(property));
    }
  }
}
