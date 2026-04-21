package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.delta.DeltaRestClientProvider;
import io.unitycatalog.client.delta.api.TablesApi;
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
 * TableCatalog#initialize} surface of {@link UCSingleCatalog}.
 *
 * <p>{@code LOAD_DELTA_CATALOG} is set to {@code false} so the internal delegate is {@code UCProxy}
 * (the only class that implements the provider in UC today). No reflection on private state; the
 * provider contract is the observable surface.
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
  public void flagEnabled_catalogOwnsCachedDeltaTablesApi() {
    // Flag on: the catalog returns a single cached TablesApi. The assignment to
    // DeltaRestClientProvider is a compile-time check that UCSingleCatalog
    // implements the interface (consumers cast to the interface to reach the
    // client). The isSameAs assertion locks in the lazy-val caching invariant;
    // if someone changes it to build per-call, this test fails.
    UCSingleCatalog catalog = catalogWithFlag(true);
    DeltaRestClientProvider provider = catalog;
    TablesApi first = provider.getDeltaTablesApi();
    assertThat(first).isNotNull();
    assertThat(provider.getDeltaTablesApi()).isSameAs(first);
  }

  @Test
  public void eachCatalogOwnsItsOwnDeltaTablesApi() {
    // Two independently initialized catalogs produce distinct TablesApi
    // instances. Guards against an accidental singleton / global-state
    // regression that would cause clients for different UC catalogs to share
    // one HTTP client.
    UCSingleCatalog a = catalogWithFlag(true);
    UCSingleCatalog b = catalogWithFlag(true);
    assertThat(a.getDeltaTablesApi()).isNotSameAs(b.getDeltaTablesApi());
  }

  @Test
  public void flagDisabled_deltaRestCatalogIsUnavailable() {
    // Default (absent option) and explicit false both disable DRC. The
    // observable contract is that the provider returns no client, so consumers
    // route through the legacy UC API path. Removing the flag gate in UCProxy
    // would cause this test to fail.
    assertThat(catalogWithFlag(null).getDeltaTablesApi()).isNull();
    assertThat(catalogWithFlag(false).getDeltaTablesApi()).isNull();
  }

  @Test
  public void getApiClient_returnsApiClient_regardlessOfFlag() {
    // getApiClient is not flag-gated: legacy UC API consumers still need the
    // underlying client when DRC is disabled. Accidentally gating getApiClient
    // on the flag would break the non-DRC path; this test catches that.
    assertThat(catalogWithFlag(false).getApiClient()).isInstanceOf(ApiClient.class);
    assertThat(catalogWithFlag(true).getApiClient()).isInstanceOf(ApiClient.class);
  }
}
