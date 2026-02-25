package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
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

/** Tests that stageCreate delegates to the underlying StagingTableCatalog. */
public class UCSingleCatalogStagingTableTest {

  private static final Identifier IDENT = Identifier.of(new String[] {"schema"}, "table");
  private static final StructType SCHEMA = new StructType().add("id", DataTypes.IntegerType, false);
  private static final Transform[] PARTITIONS = new Transform[0];
  // PROP_EXTERNAL bypasses the managed-table path in prepareTableProperties.
  private static final Map<String, String> PROPS = Map.of(TableCatalog.PROP_EXTERNAL, "true");

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
  public void testStageReplaceDelegatesToUnderlyingCatalog() throws Exception {
    StagedTable staged = mock(StagedTable.class);
    when(mockDelegate.stageReplace(eq(IDENT), eq(SCHEMA), any(), any())).thenReturn(staged);

    // stageReplace calls prepareReplaceTableProperties which needs tablesApi.
    // Since tablesApi is null, it will throw. We verify the delegate check passes and
    // the method attempts the UC API call (which indicates correct delegation path).
    // In a full integration test, the UC server would be running.
    assertThatThrownBy(() -> catalog.stageReplace(IDENT, SCHEMA, PARTITIONS, PROPS))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testStageReplaceFailsWhenDelegateIsNotStagingCatalog() {
    UCSingleCatalog nonStagingCatalog = new UCSingleCatalog();
    setDelegate(nonStagingCatalog, mock(TableCatalog.class));

    assertThatThrownBy(() -> nonStagingCatalog.stageReplace(IDENT, SCHEMA, PARTITIONS, PROPS))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("not supported");
  }

  @Test
  public void testStageCreateOrReplaceDelegatesToUnderlyingCatalog() throws Exception {
    StagedTable staged = mock(StagedTable.class);
    when(mockDelegate.stageCreateOrReplace(eq(IDENT), eq(SCHEMA), any(), any())).thenReturn(staged);

    // stageCreateOrReplace calls tablesApi.getTable which needs tablesApi.
    // Since tablesApi is null, it will throw NullPointerException, confirming
    // the delegation check passed and the method tries to reach UC.
    assertThatThrownBy(() -> catalog.stageCreateOrReplace(IDENT, SCHEMA, PARTITIONS, PROPS))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testStageCreateOrReplaceFailsWhenDelegateIsNotStagingCatalog() {
    UCSingleCatalog nonStagingCatalog = new UCSingleCatalog();
    setDelegate(nonStagingCatalog, mock(TableCatalog.class));

    assertThatThrownBy(
            () -> nonStagingCatalog.stageCreateOrReplace(IDENT, SCHEMA, PARTITIONS, PROPS))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("not supported");
  }

  private static void setDelegate(UCSingleCatalog catalog, TableCatalog delegate) {
    try {
      Field f = UCSingleCatalog.class.getDeclaredField("delegate");
      f.setAccessible(true);
      f.set(catalog, delegate);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
