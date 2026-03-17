package io.unitycatalog.spark

import io.unitycatalog.client.ApiClient
import io.unitycatalog.client.api.{SchemasApi, TablesApi, TemporaryCredentialsApi}
import io.unitycatalog.client.auth.TokenProvider
import io.unitycatalog.client.model.{ListSchemasResponse, ListTablesResponse, SchemaInfo, TableInfo}
import java.net.URI
import java.util.Collections
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsNamespaces, TableCatalog}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers.{eq => meq, isNull}
import org.mockito.Mockito.{mock, verify, when}

/** Tests that UCProxy correctly paginates through listTables and listNamespaces responses. */
class UCProxySuite {

  private val CatalogName = "test_catalog"
  private val SchemaName = "test_schema"
  private val Namespace = Array(SchemaName)

  private var mockTablesApi: TablesApi = _
  private var mockSchemasApi: SchemasApi = _
  private var proxy: TableCatalog = _
  private var proxyNs: SupportsNamespaces = _

  @BeforeEach
  def setUp(): Unit = {
    mockTablesApi = mock(classOf[TablesApi])
    mockSchemasApi = mock(classOf[SchemasApi])
    val mockApiClient = mock(classOf[ApiClient])
    val mockTokenProvider = mock(classOf[TokenProvider])
    val mockTempCredApi = mock(classOf[TemporaryCredentialsApi])

    val proxyObj = new UCProxy(
      new URI("http://localhost:8080"),
      mockTokenProvider,
      false,
      false,
      mockApiClient,
      mockTablesApi,
      mockTempCredApi)

    proxy = proxyObj
    proxy.initialize(CatalogName, new CaseInsensitiveStringMap(Collections.emptyMap()))

    // Inject mock schemasApi (initialize() creates a real one from apiClient)
    val schemasField = proxyObj.getClass.getDeclaredField("schemasApi")
    schemasField.setAccessible(true)
    schemasField.set(proxyObj, mockSchemasApi)

    proxyNs = proxyObj
  }

  // -- listTables tests --

  @Test
  def testListTablesSinglePage(): Unit = {
    val response = new ListTablesResponse()
      .tables(java.util.List.of(new TableInfo().name("table1"), new TableInfo().name("table2")))
      .nextPageToken(null)
    when(mockTablesApi.listTables(meq(CatalogName), meq(SchemaName), meq(0), isNull()))
      .thenReturn(response)

    val result = proxy.listTables(Namespace)

    assertThat(result).hasSize(2)
    assertThat(result(0).name()).isEqualTo("table1")
    assertThat(result(1).name()).isEqualTo("table2")
  }

  @Test
  def testListTablesMultiplePages(): Unit = {
    val page1 = new ListTablesResponse()
      .tables(java.util.List.of(new TableInfo().name("table1"), new TableInfo().name("table2")))
      .nextPageToken("token1")
    val page2 = new ListTablesResponse()
      .tables(java.util.List.of(new TableInfo().name("table3")))
      .nextPageToken(null)

    when(mockTablesApi.listTables(meq(CatalogName), meq(SchemaName), meq(0), isNull()))
      .thenReturn(page1)
    when(mockTablesApi.listTables(meq(CatalogName), meq(SchemaName), meq(0), meq("token1")))
      .thenReturn(page2)

    val result = proxy.listTables(Namespace)

    assertThat(result).hasSize(3)
    assertThat(result(0).name()).isEqualTo("table1")
    assertThat(result(1).name()).isEqualTo("table2")
    assertThat(result(2).name()).isEqualTo("table3")

    verify(mockTablesApi).listTables(meq(CatalogName), meq(SchemaName), meq(0), isNull())
    verify(mockTablesApi).listTables(meq(CatalogName), meq(SchemaName), meq(0), meq("token1"))
  }

  @Test
  def testListTablesEmptyResult(): Unit = {
    val response = new ListTablesResponse()
      .tables(Collections.emptyList())
      .nextPageToken(null)
    when(mockTablesApi.listTables(meq(CatalogName), meq(SchemaName), meq(0), isNull()))
      .thenReturn(response)

    val result = proxy.listTables(Namespace)

    assertThat(result).isEmpty()
  }

  // -- listNamespaces tests --

  @Test
  def testListNamespacesSinglePage(): Unit = {
    val response = new ListSchemasResponse()
      .schemas(java.util.List.of(new SchemaInfo().name("schema1"), new SchemaInfo().name("schema2")))
      .nextPageToken(null)
    when(mockSchemasApi.listSchemas(meq(CatalogName), meq(0), isNull()))
      .thenReturn(response)

    val result = proxyNs.listNamespaces()

    assertThat(result).hasNumberOfRows(2)
    assertThat(result(0)).containsExactly("schema1")
    assertThat(result(1)).containsExactly("schema2")
  }

  @Test
  def testListNamespacesMultiplePages(): Unit = {
    val page1 = new ListSchemasResponse()
      .schemas(java.util.List.of(new SchemaInfo().name("schema1"), new SchemaInfo().name("schema2")))
      .nextPageToken("token1")
    val page2 = new ListSchemasResponse()
      .schemas(java.util.List.of(new SchemaInfo().name("schema3")))
      .nextPageToken(null)

    when(mockSchemasApi.listSchemas(meq(CatalogName), meq(0), isNull()))
      .thenReturn(page1)
    when(mockSchemasApi.listSchemas(meq(CatalogName), meq(0), meq("token1")))
      .thenReturn(page2)

    val result = proxyNs.listNamespaces()

    assertThat(result).hasNumberOfRows(3)
    assertThat(result(0)).containsExactly("schema1")
    assertThat(result(1)).containsExactly("schema2")
    assertThat(result(2)).containsExactly("schema3")

    verify(mockSchemasApi).listSchemas(meq(CatalogName), meq(0), isNull())
    verify(mockSchemasApi).listSchemas(meq(CatalogName), meq(0), meq("token1"))
  }
}
