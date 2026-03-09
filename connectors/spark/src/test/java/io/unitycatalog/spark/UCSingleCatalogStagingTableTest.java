package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Tests that stageCreate delegates to the underlying StagingTableCatalog. */
public class UCSingleCatalogStagingTableTest {

  private static final Identifier IDENT = Identifier.of(new String[] {"schema"}, "table");
  private static final StructType SCHEMA = new StructType().add("id", DataTypes.IntegerType, false);
  private static final Transform[] PARTITIONS = new Transform[0];
  // PROP_EXTERNAL bypasses the managed-table path in prepareTableProperties.
  private static final Map<String, String> PROPS = Map.of(TableCatalog.PROP_EXTERNAL, "true");
  private static final Map<String, String> MANAGED_DELTA_PROPS =
      Map.of(
          TableCatalog.PROP_PROVIDER,
          "delta",
          UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
          UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);
  private static final Map<String, String> REPLACE_DELTA_PROPS =
      Map.of(TableCatalog.PROP_PROVIDER, "delta");

  private UCSingleCatalog catalog;
  private StagingTableCatalog mockDelegate;

  @BeforeEach
  public void setUp() {
    catalog = new UCSingleCatalog();
    mockDelegate = mock(StagingTableCatalog.class);
    setDelegate(catalog, mockDelegate);
  }

  @Test
  public void testStageCreateDelegatesToUnderlyingCatalog() throws Exception {
    StagedTable staged = mock(StagedTable.class);
    when(mockDelegate.stageCreate(eq(IDENT), eq(SCHEMA), any(), any())).thenReturn(staged);

    StagedTable result = catalog.stageCreate(IDENT, SCHEMA, PARTITIONS, PROPS);

    verify(mockDelegate).stageCreate(eq(IDENT), eq(SCHEMA), any(), any());
    assertThat(result).isSameAs(staged);
  }

  @Test
  public void testStageCreateFailsWhenDelegateIsNotStagingCatalog() {
    UCSingleCatalog nonStagingCatalog = new UCSingleCatalog();
    setDelegate(nonStagingCatalog, mock(TableCatalog.class));

    assertThatThrownBy(() -> nonStagingCatalog.stageCreate(IDENT, SCHEMA, PARTITIONS, PROPS))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("not supported");
  }

  /**
   * When LOAD_DELTA_CATALOG is false, initialize() sets the delegate to UCProxy directly (a plain
   * TableCatalog, not a StagingTableCatalog). Calling stageCreate should throw
   * UnsupportedOperationException because UCProxy does not implement StagingTableCatalog.
   */
  @Test
  public void testStageCreateFailWhenNoDeltaCatalog() {
    UCSingleCatalog.LOAD_DELTA_CATALOG().set(false);
    try {
      UCSingleCatalog proxyCatalog = new UCSingleCatalog();

      proxyCatalog.initialize(
          "proxyCatalog",
          new CaseInsensitiveStringMap(
              Map.of("uri", "http://localhost:0", "token", "dummy-token")));

      assertThatThrownBy(() -> proxyCatalog.stageCreate(IDENT, SCHEMA, PARTITIONS, PROPS))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("CREATE TABLE AS SELECT (CTAS) is not supported");
    } finally {
      UCSingleCatalog.LOAD_DELTA_CATALOG().set(true);
    }
  }

  @Test
  public void testStageCreateOrReplaceMissingManagedTableUsesManagedCreatePath() throws Exception {
    TablesApi mockTablesApi = mock(TablesApi.class);
    TemporaryCredentialsApi mockTempCredsApi = mock(TemporaryCredentialsApi.class);
    StagedTable staged = mock(StagedTable.class);
    when(mockDelegate.name()).thenReturn("main");
    when(mockDelegate.stageCreateOrReplace(eq(IDENT), eq(SCHEMA), any(), any())).thenReturn(staged);
    when(mockTablesApi.getTable(eq("main.schema.table"), eq(false), eq(false)))
        .thenThrow(new ApiException(404, "not found"));
    when(mockTablesApi.createStagingTable(any(CreateStagingTable.class)))
        .thenReturn(
            new StagingTableInfo().id("staging-id").stagingLocation("file:///tmp/uc-staging"));
    when(mockTempCredsApi.generateTemporaryTableCredentials(any()))
        .thenReturn(new TemporaryCredentials());

    setField(catalog, "tablesApi", mockTablesApi);
    setField(catalog, "temporaryCredentialsApi", mockTempCredsApi);
    setField(catalog, "uri", URI.create("http://localhost"));

    StagedTable result =
        catalog.stageCreateOrReplace(IDENT, SCHEMA, PARTITIONS, MANAGED_DELTA_PROPS);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass((Class) Map.class);

    verify(mockTablesApi).createStagingTable(any(CreateStagingTable.class));
    verify(mockTempCredsApi).generateTemporaryTableCredentials(any());
    verify(mockDelegate).stageCreateOrReplace(eq(IDENT), eq(SCHEMA), any(), propsCaptor.capture());
    assertThat(propsCaptor.getValue())
        .containsEntry(TableCatalog.PROP_LOCATION, "file:///tmp/uc-staging")
        .containsEntry(TableCatalog.PROP_IS_MANAGED_LOCATION, "true")
        .containsEntry(UCTableProperties.UC_TABLE_ID_KEY, "staging-id")
        .containsEntry(UCTableProperties.UC_TABLE_ID_KEY_OLD, "staging-id");
    assertThat(result).isSameAs(staged);
  }

  @Test
  public void testStageCreateOrReplaceMissingManagedTableRequiresCatalogManagedFeature()
      throws Exception {
    TablesApi mockTablesApi = mock(TablesApi.class);
    when(mockDelegate.name()).thenReturn("main");
    when(mockTablesApi.getTable(eq("main.schema.table"), eq(false), eq(false)))
        .thenThrow(new ApiException(404, "not found"));

    setField(catalog, "tablesApi", mockTablesApi);

    assertThatThrownBy(
            () ->
                catalog.stageCreateOrReplace(
                    IDENT, SCHEMA, PARTITIONS, Map.of(TableCatalog.PROP_PROVIDER, "delta")))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("Managed table creation requires table property");

    verify(mockTablesApi, never()).createStagingTable(any(CreateStagingTable.class));
    verify(mockDelegate, never()).stageCreateOrReplace(eq(IDENT), eq(SCHEMA), any(), any());
  }

  @Test
  public void testStageCreateOrReplaceExistingManagedTableAvoidsPathBasedReplace()
      throws Exception {
    TablesApi mockTablesApi = mock(TablesApi.class);
    TemporaryCredentialsApi mockTempCredsApi = mock(TemporaryCredentialsApi.class);
    StagedTable staged = mock(StagedTable.class);
    when(mockDelegate.name()).thenReturn("main");
    when(mockDelegate.stageCreateOrReplace(eq(IDENT), eq(SCHEMA), any(), any())).thenReturn(staged);
    when(mockTablesApi.getTable(eq("main.schema.table"), eq(false), eq(false)))
        .thenReturn(
            new TableInfo()
                .tableType(TableType.MANAGED)
                .dataSourceFormat(DataSourceFormat.DELTA)
                .storageLocation("file:///tmp/uc-managed-table")
                .tableId("table-id")
                .properties(
                    Map.of(
                        UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
                        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE)));
    when(mockTempCredsApi.generateTemporaryTableCredentials(any()))
        .thenReturn(new TemporaryCredentials());

    setField(catalog, "tablesApi", mockTablesApi);
    setField(catalog, "temporaryCredentialsApi", mockTempCredsApi);
    setField(catalog, "uri", URI.create("http://localhost"));

    StagedTable result =
        catalog.stageCreateOrReplace(IDENT, SCHEMA, PARTITIONS, REPLACE_DELTA_PROPS);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass((Class) Map.class);

    verify(mockTablesApi, never()).createStagingTable(any(CreateStagingTable.class));
    verify(mockTempCredsApi).generateTemporaryTableCredentials(any());
    verify(mockDelegate).stageCreateOrReplace(eq(IDENT), eq(SCHEMA), any(), propsCaptor.capture());
    assertThat(propsCaptor.getValue())
        .doesNotContainKey(TableCatalog.PROP_LOCATION)
        .containsEntry(TableCatalog.PROP_IS_MANAGED_LOCATION, "true")
        .doesNotContainKey(UCTableProperties.UC_TABLE_ID_KEY)
        .doesNotContainKey(UCTableProperties.UC_TABLE_ID_KEY_OLD)
        .containsEntry(
            UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
            UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);
    assertThat(result).isSameAs(staged);
  }

  @Test
  public void testStageReplaceExistingManagedTableAvoidsPathBasedReplace() throws Exception {
    TablesApi mockTablesApi = mock(TablesApi.class);
    TemporaryCredentialsApi mockTempCredsApi = mock(TemporaryCredentialsApi.class);
    StagedTable staged = mock(StagedTable.class);
    when(mockDelegate.name()).thenReturn("main");
    when(mockDelegate.stageReplace(eq(IDENT), eq(SCHEMA), any(), any())).thenReturn(staged);
    when(mockTablesApi.getTable(eq("main.schema.table"), eq(false), eq(false)))
        .thenReturn(
            new TableInfo()
                .tableType(TableType.MANAGED)
                .dataSourceFormat(DataSourceFormat.DELTA)
                .storageLocation("file:///tmp/uc-managed-table")
                .tableId("table-id")
                .properties(
                    Map.of(
                        UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
                        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE)));
    when(mockTempCredsApi.generateTemporaryTableCredentials(any()))
        .thenReturn(new TemporaryCredentials());

    setField(catalog, "tablesApi", mockTablesApi);
    setField(catalog, "temporaryCredentialsApi", mockTempCredsApi);
    setField(catalog, "uri", URI.create("http://localhost"));

    StagedTable result = catalog.stageReplace(IDENT, SCHEMA, PARTITIONS, REPLACE_DELTA_PROPS);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass((Class) Map.class);

    verify(mockTempCredsApi).generateTemporaryTableCredentials(any());
    verify(mockDelegate).stageReplace(eq(IDENT), eq(SCHEMA), any(), propsCaptor.capture());
    assertThat(propsCaptor.getValue())
        .doesNotContainKey(TableCatalog.PROP_LOCATION)
        .containsEntry(TableCatalog.PROP_IS_MANAGED_LOCATION, "true")
        .doesNotContainKey(UCTableProperties.UC_TABLE_ID_KEY)
        .doesNotContainKey(UCTableProperties.UC_TABLE_ID_KEY_OLD)
        .containsEntry(
            UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
            UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);
    assertThat(result).isSameAs(staged);
  }

  private static void setDelegate(UCSingleCatalog catalog, TableCatalog delegate) {
    setField(catalog, "delegate", delegate);
  }

  private static void setField(UCSingleCatalog catalog, String fieldName, Object value) {
    try {
      Field f = UCSingleCatalog.class.getDeclaredField(fieldName);
      f.setAccessible(true);
      f.set(catalog, value);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
