package io.unitycatalog.spark;

import io.unitycatalog.server.utils.ServerProperties;
import java.io.File;
import lombok.SneakyThrows;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs the {@link IcebergTableReadWriteTest} suite against external Iceberg tables: each table is
 * created with an explicit {@code LOCATION} under a local temp directory, so Unity Catalog's
 * Iceberg REST catalog registers it as EXTERNAL. Mirrors {@code DeltaExternalTableReadWriteTest}.
 *
 * <p>Only the local {@code file://} scheme is exercised (the shared base uses a file warehouse);
 * the cloud schemes the UC-connector external tests cover would need an S3-protocol backend, which
 * is out of scope here.
 */
public class IcebergExternalTableReadWriteTest extends IcebergTableReadWriteTest {

  @TempDir protected File dataDir;

  @Override
  @SneakyThrows
  protected void setUpProperties() {
    super.setUpProperties();
    // The tables live under dataDir, outside testDirectoryRoot; allow it as a local root, spelled
    // as getLocation spells the table paths.
    String key = ServerProperties.Property.EXTERNAL_LOCAL_ROOTS.getKey();
    serverProperties.setProperty(
        key, serverProperties.getProperty(key) + "," + dataDir.getCanonicalPath());
  }

  @Override
  protected boolean isManagedTable() {
    return false;
  }

  @SneakyThrows
  private String getLocation(TableSetupOptions options) {
    String rawCatalogName = options.getCatalogName().replace("`", "");
    String rawTableName = options.getTableName().replace("`", "");
    return new File(new File(dataDir, rawCatalogName), rawTableName).getCanonicalPath();
  }

  @Override
  protected String setupTable(TableSetupOptions options) {
    sql(options.createExternalTableSql(getLocation(options)));
    return options.fullTableName();
  }
}
