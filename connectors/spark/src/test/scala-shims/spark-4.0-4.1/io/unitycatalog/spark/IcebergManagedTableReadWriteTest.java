package io.unitycatalog.spark;

/**
 * Runs the {@link IcebergTableReadWriteTest} suite against managed Iceberg tables: the table has no
 * client-supplied location, so Unity Catalog's Iceberg REST catalog assigns one under the managed
 * storage root. Mirrors {@code DeltaManagedTableReadWriteTest}.
 */
public class IcebergManagedTableReadWriteTest extends IcebergTableReadWriteTest {

  @Override
  protected boolean isManagedTable() {
    return true;
  }

  @Override
  protected String setupTable(TableSetupOptions options) {
    sql(options.createManagedTableSql());
    return options.fullTableName();
  }
}
