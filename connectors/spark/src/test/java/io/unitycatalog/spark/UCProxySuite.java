package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.ListSchemasResponse;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.TableInfo;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Parameter;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for UCProxy pagination using mocked API responses.
 *
 * <p>UCProxy is a private Scala class, so Java cannot instantiate it directly. We use reflection
 * with dynamic arg resolution to handle constructor changes across branches.
 *
 * <p>End-to-end integration tests that exercise pagination through UCSingleCatalog and a real UC
 * server are in {@link BaseTableReadWriteTest#testListTablesPagination} and {@link
 * SchemaOperationsTest#testListSchemasPagination}.
 */
public class UCProxySuite {

  private static final String CATALOG_NAME = "test_catalog";
  private static final String SCHEMA_NAME = "test_schema";
  private static final String[] NAMESPACE = new String[] {SCHEMA_NAME};

  private TablesApi mockTablesApi;
  private SchemasApi mockSchemasApi;
  private TableCatalog proxy;
  private SupportsNamespaces proxyNs;

  @BeforeEach
  public void setUp() throws Exception {
    mockTablesApi = mock(TablesApi.class);
    mockSchemasApi = mock(SchemasApi.class);
    ApiClient mockApiClient = mock(ApiClient.class);
    TokenProvider mockTokenProvider = mock(TokenProvider.class);
    TemporaryCredentialsApi mockTempCredApi = mock(TemporaryCredentialsApi.class);

    // UCProxy is a private Scala class — Java cannot call `new UCProxy(...)` directly.
    // We use reflection with dynamic arg resolution so the test adapts when the
    // constructor gains or loses boolean flags across branches.
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

    Object proxyObj = ctor.newInstance(args);

    proxy = (TableCatalog) proxyObj;
    proxy.initialize(CATALOG_NAME, new CaseInsensitiveStringMap(Collections.emptyMap()));

    // Inject mock schemasApi (initialize() creates a real one from apiClient)
    Field schemasField = proxyClass.getDeclaredField("schemasApi");
    schemasField.setAccessible(true);
    schemasField.set(proxyObj, mockSchemasApi);

    proxyNs = (SupportsNamespaces) proxyObj;
  }

  // -- listTables tests --

  @Test
  public void testListTablesSinglePage() throws Exception {
    ListTablesResponse response =
        new ListTablesResponse()
            .tables(List.of(new TableInfo().name("table1"), new TableInfo().name("table2")))
            .nextPageToken(null);
    when(mockTablesApi.listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull()))
        .thenReturn(response);

    Identifier[] result = proxy.listTables(NAMESPACE);

    assertThat(result).hasSize(2);
    assertThat(result[0]).isEqualTo(Identifier.of(NAMESPACE, "table1"));
    assertThat(result[1]).isEqualTo(Identifier.of(NAMESPACE, "table2"));
  }

  @Test
  public void testListTablesMultiplePages() throws Exception {
    ListTablesResponse page1 =
        new ListTablesResponse()
            .tables(List.of(new TableInfo().name("table1"), new TableInfo().name("table2")))
            .nextPageToken("token1");
    ListTablesResponse page2 =
        new ListTablesResponse()
            .tables(List.of(new TableInfo().name("table3")))
            .nextPageToken(null);

    when(mockTablesApi.listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull()))
        .thenReturn(page1);
    when(mockTablesApi.listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), eq("token1")))
        .thenReturn(page2);

    Identifier[] result = proxy.listTables(NAMESPACE);

    assertThat(result).hasSize(3);
    assertThat(result[0]).isEqualTo(Identifier.of(NAMESPACE, "table1"));
    assertThat(result[1]).isEqualTo(Identifier.of(NAMESPACE, "table2"));
    assertThat(result[2]).isEqualTo(Identifier.of(NAMESPACE, "table3"));

    verify(mockTablesApi).listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull());
    verify(mockTablesApi).listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), eq("token1"));
  }

  @Test
  public void testListTablesEmptyResult() throws Exception {
    ListTablesResponse response =
        new ListTablesResponse().tables(Collections.emptyList()).nextPageToken(null);
    when(mockTablesApi.listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull()))
        .thenReturn(response);

    Identifier[] result = proxy.listTables(NAMESPACE);

    assertThat(result).isEmpty();
  }

  @Test
  public void testListTablesEmptyStringPageTokenStopsPagination() throws Exception {
    ListTablesResponse response =
        new ListTablesResponse().tables(List.of(new TableInfo().name("table1"))).nextPageToken("");
    when(mockTablesApi.listTables(eq(CATALOG_NAME), eq(SCHEMA_NAME), eq(0), isNull()))
        .thenReturn(response);

    Identifier[] result = proxy.listTables(NAMESPACE);

    assertThat(result).hasSize(1);
    assertThat(result[0]).isEqualTo(Identifier.of(NAMESPACE, "table1"));
  }

  // -- listNamespaces tests --

  @Test
  public void testListNamespacesSinglePage() throws Exception {
    ListSchemasResponse response =
        new ListSchemasResponse()
            .schemas(List.of(new SchemaInfo().name("schema1"), new SchemaInfo().name("schema2")))
            .nextPageToken(null);
    when(mockSchemasApi.listSchemas(eq(CATALOG_NAME), eq(0), isNull())).thenReturn(response);

    String[][] result = proxyNs.listNamespaces();

    assertThat(result).hasNumberOfRows(2);
    assertThat(result[0]).containsExactly("schema1");
    assertThat(result[1]).containsExactly("schema2");
  }

  @Test
  public void testListNamespacesMultiplePages() throws Exception {
    ListSchemasResponse page1 =
        new ListSchemasResponse()
            .schemas(List.of(new SchemaInfo().name("schema1"), new SchemaInfo().name("schema2")))
            .nextPageToken("token1");
    ListSchemasResponse page2 =
        new ListSchemasResponse()
            .schemas(List.of(new SchemaInfo().name("schema3")))
            .nextPageToken(null);

    when(mockSchemasApi.listSchemas(eq(CATALOG_NAME), eq(0), isNull())).thenReturn(page1);
    when(mockSchemasApi.listSchemas(eq(CATALOG_NAME), eq(0), eq("token1"))).thenReturn(page2);

    String[][] result = proxyNs.listNamespaces();

    assertThat(result).hasNumberOfRows(3);
    assertThat(result[0]).containsExactly("schema1");
    assertThat(result[1]).containsExactly("schema2");
    assertThat(result[2]).containsExactly("schema3");

    verify(mockSchemasApi).listSchemas(eq(CATALOG_NAME), eq(0), isNull());
    verify(mockSchemasApi).listSchemas(eq(CATALOG_NAME), eq(0), eq("token1"));
  }

  @Test
  public void testListNamespacesEmptyStringPageTokenStopsPagination() throws Exception {
    ListSchemasResponse response =
        new ListSchemasResponse()
            .schemas(List.of(new SchemaInfo().name("schema1")))
            .nextPageToken("");
    when(mockSchemasApi.listSchemas(eq(CATALOG_NAME), eq(0), isNull())).thenReturn(response);

    String[][] result = proxyNs.listNamespaces();

    assertThat(result).hasNumberOfRows(1);
    assertThat(result[0]).containsExactly("schema1");
  }
}
