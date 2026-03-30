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
          UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
          UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);
  private static final Map<String, String> MANAGED_DELTA_REPLACE_OVERRIDE_PROPS =
      Map.of(
          TableCatalog.PROP_PROVIDER,
          "delta",
          UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
          "disabled");
  private static final Map<String, String> REPLACE_DELTA_PROPS =
      Map.of(TableCatalog.PROP_PROVIDER, "delta");
  private static final String EXTERNAL_LOCATION = "file:///tmp/uc-external-table";
  private static final String MANAGED_LOCATION = "file:///tmp/uc-managed-table";

  private static final class ManagedReplaceMocks {
    final TablesApi tablesApi = mock(TablesApi.class);
    final TemporaryCredentialsApi tempCredsApi = mock(TemporaryCredentialsApi.class);
    final StagedTable staged = mock(StagedTable.class);
  }

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
    ManagedReplaceMocks mocks = new ManagedReplaceMocks();
    mockMissingTableLookup(mocks.tablesApi);
    when(mockDelegate.stageCreateOrReplace(eq(IDENT), eq(SCHEMA), any(), any()))
        .thenReturn(mocks.staged);
    when(mocks.tablesApi.createStagingTable(any(CreateStagingTable.class)))
        .thenReturn(
            new StagingTableInfo().id("staging-id").stagingLocation("file:///tmp/uc-staging"));
    setTemporaryCredentialsApi(mocks.tempCredsApi);

    StagedTable result =
        catalog.stageCreateOrReplace(IDENT, SCHEMA, PARTITIONS, MANAGED_DELTA_PROPS);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass((Class) Map.class);

    verify(mocks.tablesApi).createStagingTable(any(CreateStagingTable.class));
    verify(mocks.tempCredsApi).generateTemporaryTableCredentials(any());
    verify(mockDelegate).stageCreateOrReplace(eq(IDENT), eq(SCHEMA), any(), propsCaptor.capture());
    assertThat(propsCaptor.getValue())
        .containsEntry(TableCatalog.PROP_LOCATION, "file:///tmp/uc-staging")
        .containsEntry(TableCatalog.PROP_IS_MANAGED_LOCATION, "true")
        .containsEntry(UCTableProperties.UC_TABLE_ID_KEY, "staging-id");
    assertThat(result).isSameAs(mocks.staged);
  }

  @Test
  public void testStageCreateOrReplaceMissingManagedTableRequiresCatalogManagedFeature()
      throws Exception {
    TablesApi mockTablesApi = mock(TablesApi.class);
    mockMissingTableLookup(mockTablesApi);

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
    assertExistingManagedTableReplaceUsesManagedProps(true);
  }

  @Test
  public void testStageReplaceExistingManagedTableAvoidsPathBasedReplace() throws Exception {
    assertExistingManagedTableReplaceUsesManagedProps(false);
  }

  @Test
  public void testStageReplaceExistingManagedTableRejectsUserSpecifiedLocation() throws Exception {
    TablesApi mockTablesApi = mockExistingTableLookup(existingManagedDeltaTableInfo());

    assertThatThrownBy(
            () ->
                catalog.stageReplace(
                    IDENT,
                    SCHEMA,
                    PARTITIONS,
                    Map.of(
                        TableCatalog.PROP_PROVIDER,
                        "delta",
                        TableCatalog.PROP_LOCATION,
                        "file:///tmp/other-location")))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("cannot specify property 'location'");

    verify(mockDelegate, never()).stageReplace(eq(IDENT), eq(SCHEMA), any(), any());
  }

  @Test
  public void testStageReplaceExistingManagedTablePreservesUserProperties() throws Exception {
    ManagedReplaceMocks mocks = mockExistingManagedReplace(false);

    catalog.stageReplace(
        IDENT,
        SCHEMA,
        PARTITIONS,
        Map.of(TableCatalog.PROP_PROVIDER, "delta", "delta.appendOnly", "true"));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass((Class) Map.class);

    verify(mockDelegate).stageReplace(eq(IDENT), eq(SCHEMA), any(), propsCaptor.capture());
    assertThat(propsCaptor.getValue())
        .containsEntry(TableCatalog.PROP_PROVIDER, "delta")
        .containsEntry("delta.appendOnly", "true");
    verify(mocks.tempCredsApi).generateTemporaryTableCredentials(any());
  }

  @Test
  public void testStageReplaceExistingManagedTableInjectsDeltaProviderWhenOmitted()
      throws Exception {
    ManagedReplaceMocks mocks = mockExistingManagedReplace(false);

    catalog.stageReplace(
        IDENT,
        SCHEMA,
        PARTITIONS,
        Map.of(
            UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
            UCTableProperties.DELTA_CATALOG_MANAGED_VALUE));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass((Class) Map.class);

    verify(mockDelegate).stageReplace(eq(IDENT), eq(SCHEMA), any(), propsCaptor.capture());
    assertThat(propsCaptor.getValue())
        .containsEntry(TableCatalog.PROP_PROVIDER, "delta")
        .containsEntry(
            UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
            UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);
    verify(mocks.tempCredsApi).generateTemporaryTableCredentials(any());
  }

  @Test
  public void testStageReplaceExistingManagedTableRejectsProviderChange() throws Exception {
    mockExistingManagedReplace(false);

    assertThatThrownBy(
            () ->
                catalog.stageReplace(
                    IDENT,
                    SCHEMA,
                    PARTITIONS,
                    Map.of(
                        TableCatalog.PROP_PROVIDER,
                        "parquet",
                        UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
                        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE)))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("requires USING DELTA")
        .hasMessageContaining("Cannot change table format from DELTA to PARQUET");

    verify(mockDelegate, never()).stageReplace(eq(IDENT), eq(SCHEMA), any(), any());
  }

  @Test
  public void testStageCreateOrReplaceExistingManagedTableInjectsDeltaProviderWhenOmitted()
      throws Exception {
    ManagedReplaceMocks mocks = mockExistingManagedReplace(true);

    catalog.stageCreateOrReplace(
        IDENT,
        SCHEMA,
        PARTITIONS,
        Map.of(
            UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
            UCTableProperties.DELTA_CATALOG_MANAGED_VALUE));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass((Class) Map.class);

    verify(mockDelegate).stageCreateOrReplace(eq(IDENT), eq(SCHEMA), any(), propsCaptor.capture());
    assertThat(propsCaptor.getValue())
        .containsEntry(TableCatalog.PROP_PROVIDER, "delta")
        .containsEntry(
            UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
            UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);
    verify(mocks.tempCredsApi).generateTemporaryTableCredentials(any());
  }

  @Test
  public void testStageCreateOrReplaceExistingManagedTableRejectsProviderChange() throws Exception {
    mockExistingManagedReplace(true);

    assertThatThrownBy(
            () ->
                catalog.stageCreateOrReplace(
                    IDENT,
                    SCHEMA,
                    PARTITIONS,
                    Map.of(
                        TableCatalog.PROP_PROVIDER,
                        "parquet",
                        UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
                        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE)))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("requires USING DELTA")
        .hasMessageContaining("Cannot change table format from DELTA to PARQUET");

    verify(mockDelegate, never()).stageCreateOrReplace(eq(IDENT), eq(SCHEMA), any(), any());
  }

  private void assertExistingManagedTableReplaceUsesManagedProps(boolean createOrReplace)
      throws Exception {
    ManagedReplaceMocks mocks = mockExistingManagedReplace(createOrReplace);

    StagedTable result =
        createOrReplace
            ? catalog.stageCreateOrReplace(IDENT, SCHEMA, PARTITIONS, REPLACE_DELTA_PROPS)
            : catalog.stageReplace(IDENT, SCHEMA, PARTITIONS, REPLACE_DELTA_PROPS);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass((Class) Map.class);

    verify(mocks.tablesApi, never()).createStagingTable(any(CreateStagingTable.class));
    verify(mocks.tempCredsApi).generateTemporaryTableCredentials(any());
    if (createOrReplace) {
      verify(mockDelegate)
          .stageCreateOrReplace(eq(IDENT), eq(SCHEMA), any(), propsCaptor.capture());
    } else {
      verify(mockDelegate).stageReplace(eq(IDENT), eq(SCHEMA), any(), propsCaptor.capture());
    }
    assertThat(propsCaptor.getValue())
        .containsEntry(TableCatalog.PROP_PROVIDER, "delta")
        .doesNotContainKey(TableCatalog.PROP_LOCATION)
        .containsEntry(TableCatalog.PROP_IS_MANAGED_LOCATION, "true")
        .doesNotContainKey(UCTableProperties.UC_TABLE_ID_KEY)
        .containsEntry(
            UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
            UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);
    assertThat(result).isSameAs(mocks.staged);
  }

  private ManagedReplaceMocks mockExistingManagedReplace(boolean createOrReplace) throws Exception {
    ManagedReplaceMocks mocks = new ManagedReplaceMocks();
    mockExistingTableLookup(mocks.tablesApi, existingManagedDeltaTableInfo());
    setTemporaryCredentialsApi(mocks.tempCredsApi);
    if (createOrReplace) {
      when(mockDelegate.stageCreateOrReplace(eq(IDENT), eq(SCHEMA), any(), any()))
          .thenReturn(mocks.staged);
    } else {
      when(mockDelegate.stageReplace(eq(IDENT), eq(SCHEMA), any(), any())).thenReturn(mocks.staged);
    }
    return mocks;
  }

  private static TableInfo existingManagedDeltaTableInfo() {
    return new TableInfo()
        .tableType(TableType.MANAGED)
        .dataSourceFormat(DataSourceFormat.DELTA)
        .storageLocation("file:///tmp/uc-managed-table")
        .tableId("table-id")
        .properties(
            Map.of(
                UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
                UCTableProperties.DELTA_CATALOG_MANAGED_VALUE));
  }

  @Test
  public void testStageReplaceExistingManagedTableRejectsCatalogManagedOverrides()
      throws Exception {
    TablesApi mockTablesApi = mockExistingTableLookup(existingManagedDeltaTableInfo());

    assertThatThrownBy(
            () ->
                catalog.stageReplace(
                    IDENT, SCHEMA, PARTITIONS, MANAGED_DELTA_REPLACE_OVERRIDE_PROPS))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining(
            "Cannot override property '" + UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW + "'");

    verify(mockDelegate, never()).stageReplace(eq(IDENT), eq(SCHEMA), any(), any());
  }

  @Test
  public void testStageReplaceExistingManagedNonCatalogManagedTableIsRejected() throws Exception {
    TablesApi mockTablesApi =
        mockExistingTableLookup(existingDeltaTableInfo(TableType.MANAGED, Map.of()));

    assertThatThrownBy(() -> catalog.stageReplace(IDENT, SCHEMA, PARTITIONS, REPLACE_DELTA_PROPS))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("only supported for catalog-managed UC Delta tables");

    verify(mockDelegate, never()).stageReplace(eq(IDENT), eq(SCHEMA), any(), any());
  }

  @Test
  public void testStageReplaceExistingExternalTableIsRejected() throws Exception {
    TablesApi mockTablesApi =
        mockExistingTableLookup(existingDeltaTableInfo(TableType.EXTERNAL, null));

    assertThatThrownBy(() -> catalog.stageReplace(IDENT, SCHEMA, PARTITIONS, REPLACE_DELTA_PROPS))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("only supported for catalog-managed UC Delta tables");

    verify(mockDelegate, never()).stageReplace(eq(IDENT), eq(SCHEMA), any(), any());
  }

  @Test
  public void testStageCreateOrReplaceExistingExternalTableIsRejected() throws Exception {
    TablesApi mockTablesApi =
        mockExistingTableLookup(existingDeltaTableInfo(TableType.EXTERNAL, null));

    assertThatThrownBy(
            () -> catalog.stageCreateOrReplace(IDENT, SCHEMA, PARTITIONS, REPLACE_DELTA_PROPS))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("only supported for catalog-managed UC Delta tables");

    verify(mockDelegate, never()).stageCreateOrReplace(eq(IDENT), eq(SCHEMA), any(), any());
  }

  private TablesApi mockExistingTableLookup(TableInfo tableInfo) throws Exception {
    TablesApi tablesApi = mock(TablesApi.class);
    mockExistingTableLookup(tablesApi, tableInfo);
    return tablesApi;
  }

  private void mockExistingTableLookup(TablesApi tablesApi, TableInfo tableInfo) throws Exception {
    when(mockDelegate.name()).thenReturn("main");
    when(tablesApi.getTable(eq("main.schema.table"), eq(false), eq(false))).thenReturn(tableInfo);
    setField(catalog, "tablesApi", tablesApi);
  }

  private void mockMissingTableLookup(TablesApi tablesApi) throws Exception {
    when(mockDelegate.name()).thenReturn("main");
    when(tablesApi.getTable(eq("main.schema.table"), eq(false), eq(false)))
        .thenThrow(new ApiException(404, "not found"));
    setField(catalog, "tablesApi", tablesApi);
  }

  private void setTemporaryCredentialsApi(TemporaryCredentialsApi tempCredsApi) throws Exception {
    when(tempCredsApi.generateTemporaryTableCredentials(any()))
        .thenReturn(new TemporaryCredentials());
    setField(catalog, "temporaryCredentialsApi", tempCredsApi);
    setField(catalog, "uri", URI.create("http://localhost"));
  }

  private static void setDelegate(UCSingleCatalog catalog, TableCatalog delegate) {
    setField(catalog, "delegate", delegate);
  }

  private static TableInfo existingDeltaTableInfo(
      TableType tableType, Map<String, String> properties) {
    TableInfo tableInfo =
        new TableInfo()
            .tableType(tableType)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .storageLocation(
                tableType == TableType.EXTERNAL ? EXTERNAL_LOCATION : MANAGED_LOCATION);
    if (tableType == TableType.MANAGED) {
      tableInfo.tableId("table-id");
    }
    if (properties != null) {
      tableInfo.properties(properties);
    }
    return tableInfo;
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
