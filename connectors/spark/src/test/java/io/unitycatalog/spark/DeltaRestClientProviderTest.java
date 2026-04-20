package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.unitycatalog.client.delta.DeltaRestClientProvider;
import io.unitycatalog.client.delta.api.TablesApi;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests DeltaRestClientProvider wiring in UCSingleCatalog and UCProxy: flag gate at UCProxy,
 * UCSingleCatalog forwards via its ucProxy field (not through delegate), and both entry points
 * return the same cached TablesApi instance.
 *
 * <p>Uses LOAD_DELTA_CATALOG=false to avoid needing a SparkSession. A non-provider mock delegate is
 * injected via reflection to simulate the production case where delegate is DeltaCatalog.
 */
public class DeltaRestClientProviderTest {

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
  public void testFlagOnBothEntryPointsReturnSameInstance() {
    UCSingleCatalog catalog = initializedCatalog(withFlag(BASE_OPTIONS, "true"));

    TablesApi fromCatalog = ((DeltaRestClientProvider) catalog).getDeltaTablesApi();
    TablesApi fromProxy = readUcProxy(catalog).getDeltaTablesApi();

    assertThat(fromCatalog).isNotNull();
    assertThat(fromProxy).isSameAs(fromCatalog);
  }

  @Test
  public void testFlagOffReturnsNullFromBothEntryPoints() {
    UCSingleCatalog catalog = initializedCatalog(withFlag(BASE_OPTIONS, "false"));

    assertThat(((DeltaRestClientProvider) catalog).getDeltaTablesApi()).isNull();
    assertThat(readUcProxy(catalog).getDeltaTablesApi()).isNull();
  }

  /**
   * Simulates the production case where UCSingleCatalog.delegate is DeltaCatalog (which does not
   * implement DeltaRestClientProvider). UCSingleCatalog must still resolve the TablesApi via its
   * ucProxy field rather than through delegate.
   */
  @Test
  public void testForwardsThroughUcProxyFieldNotDelegate() {
    UCSingleCatalog catalog = initializedCatalog(withFlag(BASE_OPTIONS, "true"));
    TableCatalog nonProviderDelegate = mock(TableCatalog.class);
    setField(catalog, "delegate", nonProviderDelegate);

    TablesApi fromCatalog = ((DeltaRestClientProvider) catalog).getDeltaTablesApi();

    assertThat(fromCatalog).isNotNull();
    assertThat(fromCatalog).isSameAs(readUcProxy(catalog).getDeltaTablesApi());
  }

  @Test
  public void testDeltaTablesApiIsCached() {
    UCSingleCatalog catalog = initializedCatalog(withFlag(BASE_OPTIONS, "true"));
    DeltaRestClientProvider provider = (DeltaRestClientProvider) catalog;

    assertThat(provider.getDeltaTablesApi()).isSameAs(provider.getDeltaTablesApi());
  }

  private static UCSingleCatalog initializedCatalog(Map<String, String> options) {
    UCSingleCatalog catalog = new UCSingleCatalog();
    catalog.initialize("unity", new CaseInsensitiveStringMap(options));
    return catalog;
  }

  private static Map<String, String> withFlag(Map<String, String> base, String value) {
    HashMap<String, String> merged = new HashMap<>(base);
    merged.put("deltaRestApi.enabled", value);
    return merged;
  }

  private static DeltaRestClientProvider readUcProxy(UCSingleCatalog catalog) {
    try {
      Field f = UCSingleCatalog.class.getDeclaredField("ucProxy");
      f.setAccessible(true);
      return (DeltaRestClientProvider) f.get(catalog);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private static void setField(UCSingleCatalog catalog, String name, Object value) {
    try {
      Field f = UCSingleCatalog.class.getDeclaredField(name);
      f.setAccessible(true);
      f.set(catalog, value);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
