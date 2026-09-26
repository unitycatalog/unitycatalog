package io.unitycatalog.server.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class ServerPropertiesCacheConfigTest {

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
}
