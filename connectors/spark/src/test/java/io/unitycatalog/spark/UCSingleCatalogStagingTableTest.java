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
