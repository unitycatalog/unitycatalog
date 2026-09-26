package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.time.Duration;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class ServerPropertiesCacheConfigTest {

  /**
   * Replicates the {@code testInvalidProperty} mechanism from {@code ServerPropertiesTest}: asserts
   * that both the constructor-based and {@code set()}-based paths reject the value with a {@link
   * BaseException} whose message contains every supplied fragment.
   */
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
  void defaultsMatchSpec() {
    ServerProperties props = new ServerProperties(new Properties());
    assertTrue(props.isStorageCredentialCacheEnabled());
    assertEquals(1000, props.getStorageCredentialCacheMaxSize());
    assertEquals(Duration.parse("PT1M"), props.getStorageCredentialCacheRenewalLeadTime());
    assertEquals(Duration.parse("PT5M"), props.getStorageCredentialCacheMaxAge());
  }

  @Test
  void overridesAreRead() {
    Properties p = new Properties();
    p.setProperty("server.storage-credential-cache.enabled", "false");
    p.setProperty("server.storage-credential-cache.max-size", "50");
    p.setProperty("server.storage-credential-cache.renewal-lead-time", "PT30S");
    p.setProperty("server.storage-credential-cache.max-age", "PT2M");
    ServerProperties props = new ServerProperties(p);

    assertFalse(props.isStorageCredentialCacheEnabled());
    assertEquals(50, props.getStorageCredentialCacheMaxSize());
    assertEquals(Duration.parse("PT30S"), props.getStorageCredentialCacheRenewalLeadTime());
    assertEquals(Duration.parse("PT2M"), props.getStorageCredentialCacheMaxAge());
  }

  @Test
  void invalidPropertiesAreRejected() {
    // max-size: zero, negative, and non-integer are all rejected
    testInvalidProperty(
        Property.STORAGE_CREDENTIAL_CACHE_MAX_SIZE,
        "0",
        "Invalid value '0'",
        "server.storage-credential-cache.max-size");
    testInvalidProperty(
        Property.STORAGE_CREDENTIAL_CACHE_MAX_SIZE,
        "-1",
        "Invalid value '-1'",
        "server.storage-credential-cache.max-size");
    testInvalidProperty(
        Property.STORAGE_CREDENTIAL_CACHE_MAX_SIZE,
        "abc",
        "Invalid value 'abc'",
        "server.storage-credential-cache.max-size");

    // renewal-lead-time: zero duration is rejected (must be at least 1 ms)
    testInvalidProperty(
        Property.STORAGE_CREDENTIAL_CACHE_RENEWAL_LEAD_TIME,
        "PT0S",
        "server.storage-credential-cache.renewal-lead-time",
        "Expected at least one millisecond");

    // max-age: zero and negative durations are rejected
    testInvalidProperty(
        Property.STORAGE_CREDENTIAL_CACHE_MAX_AGE,
        "PT0S",
        "server.storage-credential-cache.max-age",
        "Expected at least one millisecond");
    testInvalidProperty(
        Property.STORAGE_CREDENTIAL_CACHE_MAX_AGE,
        "PT-1M",
        "server.storage-credential-cache.max-age",
        "Expected at least one millisecond");

    // enabled: non-boolean string is rejected
    testInvalidProperty(
        Property.STORAGE_CREDENTIAL_CACHE_ENABLED,
        "yes",
        "Invalid value 'yes'",
        "server.storage-credential-cache.enabled",
        "Allowed values: [true, false]");
  }
}
