package io.unitycatalog.spark;

import static org.mockito.Mockito.mock;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.auth.TokenProvider;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Parameter;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Shared, version-agnostic test fixture that builds a mock-backed {@code UCProxy} for the {@code
 * UCProxy*} unit suites. Lives under {@code src/test/java/} so it compiles on Spark 4.0, 4.1, and
 * 4.2, and references no Spark-4.2-only type (the constructed proxy is exposed as a bare {@code
 * Object} so view suites can cast it to {@code RelationCatalog}/{@code ViewCatalog} themselves on
 * the 4.2 build).
 *
 * <p>This is a plain helper, NOT a test class, and is composed (not inherited) by each suite's
 * {@code @BeforeEach} so that no {@code @Test} methods are shared via inheritance (which would
 * otherwise double-run on the 4.2 build).
 *
 * <p>UCProxy is a private Scala class, so Java cannot instantiate it directly. We use reflection
 * with dynamic arg resolution so the fixture adapts when the constructor gains or loses boolean
 * flags across branches.
 */
final class UCProxyTestFixture {

  static final String CATALOG_NAME = "test_catalog";
  static final String SCHEMA_NAME = "test_schema";
  static final String[] NAMESPACE = new String[] {SCHEMA_NAME};

  final TablesApi mockTablesApi = mock(TablesApi.class);
  final SchemasApi mockSchemasApi = mock(SchemasApi.class);

  // Held as a bare Object so this fixture never names a Spark-4.2-only catalog type; the 4.2 view
  // suite casts it to RelationCatalog / ViewCatalog itself.
  Object proxyObj;
  TableCatalog proxy;
  SupportsNamespaces proxyNs;

  /** Builds the proxy, initializes it, and injects the mock {@code schemasApi}. */
  UCProxyTestFixture build() throws Exception {
    ApiClient mockApiClient = mock(ApiClient.class);
    TokenProvider mockTokenProvider = mock(TokenProvider.class);
    TemporaryCredentialsApi mockTempCredApi = mock(TemporaryCredentialsApi.class);

    Map<Class<?>, Object> argsByType = new HashMap<>();
    argsByType.put(URI.class, new URI("http://localhost:8080"));
    argsByType.put(TokenProvider.class, mockTokenProvider);
    argsByType.put(ApiClient.class, mockApiClient);
    argsByType.put(TablesApi.class, mockTablesApi);
    argsByType.put(TemporaryCredentialsApi.class, mockTempCredApi);

    Class<?> proxyClass = Class.forName("io.unitycatalog.spark.UCProxy");
    Constructor<?> ctor = proxyClass.getDeclaredConstructors()[0];
    ctor.setAccessible(true);

    Parameter[] params = ctor.getParameters();
    Object[] args = new Object[params.length];
    for (int i = 0; i < params.length; i++) {
      Class<?> type = params[i].getType();
      if (argsByType.containsKey(type)) {
        args[i] = argsByType.get(type);
      } else if (type == boolean.class) {
        args[i] = false;
      } else {
        args[i] = null;
      }
    }

    proxyObj = ctor.newInstance(args);

    proxy = (TableCatalog) proxyObj;
    proxy.initialize(CATALOG_NAME, new CaseInsensitiveStringMap(Collections.emptyMap()));

    // Inject mock schemasApi (initialize() creates a real one from apiClient).
    Field schemasField = proxyClass.getDeclaredField("schemasApi");
    schemasField.setAccessible(true);
    schemasField.set(proxyObj, mockSchemasApi);

    proxyNs = (SupportsNamespaces) proxyObj;
    return this;
  }
}
