package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.delta.DeltaRestClientProvider;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DeltaRestClientProvider} as exposed through the public {@link
 * TableCatalog#initialize} surface of {@link UCSingleCatalog}. {@code UCSingleCatalog} forwards
 * provider calls directly to its internal {@code UCProxy} field, so the tests drive the catalog
 * through public initialization and assert on the observable Optional return.
 */
public class DeltaRestClientProviderTest {

  private static final String CATALOG = "test_catalog";
  // Port 0 is fine: initialize() only constructs the client; no HTTP call fires here.
  private static final String URI = "http://localhost:0";
  private static final String TOKEN = "dummy-token";

  @BeforeEach
  public void setUp() {
    UCSingleCatalog.LOAD_DELTA_CATALOG().set(false);
  }

  @AfterEach
  public void tearDown() {
    UCSingleCatalog.LOAD_DELTA_CATALOG().set(true);
  }

  private static UCSingleCatalog catalogWithFlag(Boolean flagValue) {
    Map<String, String> opts = new HashMap<>();
    opts.put(OptionsUtil.URI, URI);
    opts.put(OptionsUtil.TOKEN, TOKEN);
    if (flagValue != null) {
      opts.put(OptionsUtil.DELTA_REST_API_ENABLED, flagValue.toString());
    }
    UCSingleCatalog catalog = new UCSingleCatalog();
    catalog.initialize(CATALOG, new CaseInsensitiveStringMap(opts));
    return catalog;
  }

  @Test
  public void flagEnabled_returnsDeltaTablesApi() {
    // Flag on: the catalog returns a present Optional carrying a TablesApi. The assignment
    // to DeltaRestClientProvider is a compile-time check that UCSingleCatalog implements
    // the interface.
    UCSingleCatalog catalog = catalogWithFlag(true);
    DeltaRestClientProvider provider = catalog;
    assertThat(provider.getDeltaTablesApi()).isPresent();
  }

  @Test
  public void eachCatalogOwnsItsOwnDeltaTablesApi() {
    // Two independently initialized catalogs have their own ApiClient and their own
    // DRC TablesApi. Guards against an accidental singleton or shared static-state
    // regression that would make different UC catalogs collide on one HTTP client.
    UCSingleCatalog a = catalogWithFlag(true);
    UCSingleCatalog b = catalogWithFlag(true);
    assertThat(a.getDeltaTablesApi().get()).isNotSameAs(b.getDeltaTablesApi().get());
    assertThat(a.getApiClient()).isNotSameAs(b.getApiClient());
  }

  @Test
  public void flagDisabled_deltaRestCatalogIsUnavailable() {
    // Default (absent option) and explicit false both disable DRC. The observable
    // contract is an empty Optional — consumers fall through to the legacy UC API
    // path via getApiClient(). Removing the flag gate in UCProxy would cause this
    // test to fail.
    assertThat(catalogWithFlag(null).getDeltaTablesApi()).isEmpty();
    assertThat(catalogWithFlag(false).getDeltaTablesApi()).isEmpty();
  }

  @Test
  public void getApiClient_returnsApiClient_regardlessOfFlag() {
    // getApiClient is not flag-gated: legacy UC API consumers still need the
    // underlying client when DRC is disabled. The isSameAs checks confirm the
    // method returns the SAME cached instance on repeated calls — catches the
    // "accidentally flag-gated, returns a fresh client each call" regression.
    UCSingleCatalog off = catalogWithFlag(false);
    ApiClient offClient = off.getApiClient();
    assertThat(offClient).isNotNull();
    assertThat(off.getApiClient()).isSameAs(offClient);

    UCSingleCatalog on = catalogWithFlag(true);
    ApiClient onClient = on.getApiClient();
    assertThat(onClient).isNotNull();
    assertThat(on.getApiClient()).isSameAs(onClient);
  }
}
