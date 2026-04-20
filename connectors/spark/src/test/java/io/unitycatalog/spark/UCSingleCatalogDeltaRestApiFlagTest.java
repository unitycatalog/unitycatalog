package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.Map;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that UCSingleCatalog reads the deltaRestApi.enabled option into its internal
 * deltaRestApiEnabled field. The flag is foundation for the Delta REST Catalog integration; no code
 * branches on it yet.
 */
public class UCSingleCatalogDeltaRestApiFlagTest {

  private static final Map<String, String> BASE_OPTIONS =
      Map.of("uri", "http://localhost:0", "token", "dummy-token");

  @BeforeEach
  public void disableDeltaCatalogLoading() {
    UCSingleCatalog.LOAD_DELTA_CATALOG().set(false);
  }

  @AfterEach
  public void restoreDeltaCatalogLoading() {
    UCSingleCatalog.LOAD_DELTA_CATALOG().set(true);
  }

  @Test
  public void testFlagDefaultsToFalseWhenUnset() {
    UCSingleCatalog catalog = initializedCatalog(BASE_OPTIONS);
    assertThat(readFlag(catalog)).isFalse();
  }

  @Test
  public void testFlagReadsTrueWhenExplicitlyTrue() {
    Map<String, String> options = withFlag(BASE_OPTIONS, "true");
    UCSingleCatalog catalog = initializedCatalog(options);
    assertThat(readFlag(catalog)).isTrue();
  }

  @Test
  public void testFlagReadsFalseWhenExplicitlyFalse() {
    Map<String, String> options = withFlag(BASE_OPTIONS, "false");
    UCSingleCatalog catalog = initializedCatalog(options);
    assertThat(readFlag(catalog)).isFalse();
  }

  private static UCSingleCatalog initializedCatalog(Map<String, String> options) {
    UCSingleCatalog catalog = new UCSingleCatalog();
    catalog.initialize("unity", new CaseInsensitiveStringMap(options));
    return catalog;
  }

  private static Map<String, String> withFlag(Map<String, String> base, String value) {
    java.util.HashMap<String, String> merged = new java.util.HashMap<>(base);
    merged.put("deltaRestApi.enabled", value);
    return merged;
  }

  private static boolean readFlag(UCSingleCatalog catalog) {
    try {
      Field f = UCSingleCatalog.class.getDeclaredField("deltaRestApiEnabled");
      f.setAccessible(true);
      return (Boolean) f.get(catalog);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
