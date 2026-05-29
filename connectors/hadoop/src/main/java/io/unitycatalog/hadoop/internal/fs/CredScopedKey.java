package io.unitycatalog.hadoop.internal.fs;

import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_CATALOG_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_DEFAULT_VALUE;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_LOCATION_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_PATH_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_PATH_OPERATION_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_TABLE_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_TABLE_OPERATION_KEY;

import io.unitycatalog.hadoop.internal.UCDeltaTableIdentifier;
import java.net.URI;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * Cache key that identifies a credential scope for {@link CredScopedFileSystem}.
 *
 * <p>There are five implementations:
 *
 * <ul>
 *   <li>{@link TableCredScopedKey} — keyed by table ID and operation; used for table-level
 *       temporary credentials via the UC credentials API.
 *   <li>{@link DeltaTableCredScopedKey} — keyed by table identity, operation, and location; used
 *       for table-level temporary credentials via the UC Delta credentials API.
 *   <li>{@link DeltaStagingTableCredScopedKey} — keyed by staging table ID and location; used for
 *       staging-table-level temporary credentials via the UC Delta credentials API.
 *   <li>{@link PathCredScopedKey} — keyed by path and operation; used for path-level temporary
 *       credentials.
 *   <li>{@link DefaultCredScopedKey} — keyed by URI scheme and authority; used as a fallback when
 *       no Unity Catalog credential type is present in the configuration.
 * </ul>
 */
public interface CredScopedKey {

  static CredScopedKey create(URI uri, Configuration conf) {
    String type = conf.get(UC_CREDENTIALS_TYPE_KEY);
    boolean isDeltaApi =
        conf.getBoolean(
            UC_DELTA_CREDENTIALS_API_ENABLED_KEY, UC_DELTA_CREDENTIALS_API_ENABLED_DEFAULT_VALUE);
    String stagingTableId = conf.get(UC_DELTA_STAGING_TABLE_ID_KEY);

    if (stagingTableId != null && !stagingTableId.isEmpty()) {
      // Case 1: Delta staging table — keyed by staging table UUID + location.
      String location = conf.get(UC_DELTA_STAGING_TABLE_LOCATION_KEY);
      return new DeltaStagingTableCredScopedKey(stagingTableId, location);

    } else if (UC_CREDENTIALS_TYPE_PATH_VALUE.equals(type)) {
      // Case 2: Path-based credentials — keyed by path + operation.
      String path = conf.get(UC_PATH_KEY);
      String pathOp = conf.get(UC_PATH_OPERATION_KEY);
      return new PathCredScopedKey(path, pathOp);

    } else if (UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type) && isDeltaApi) {
      // Case 3: Delta table — keyed by catalog.schema.table + operation + location.
      String tableOp = conf.get(UC_TABLE_OPERATION_KEY);
      UCDeltaTableIdentifier identifier =
          UCDeltaTableIdentifier.of(
              conf.get(UC_DELTA_CATALOG_KEY),
              conf.get(UC_DELTA_SCHEMA_KEY),
              conf.get(UC_DELTA_TABLE_NAME_KEY));
      String location = conf.get(UC_DELTA_LOCATION_KEY);
      return new DeltaTableCredScopedKey(identifier, tableOp, location);

    } else if (UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type)) {
      // Case 4: UC table (legacy API) — keyed by table ID + operation.
      String tableOp = conf.get(UC_TABLE_OPERATION_KEY);
      String tableId = conf.get(UC_TABLE_ID_KEY);
      return new TableCredScopedKey(tableId, tableOp);

    } else {
      // Case 5: Fallback — keyed by URI scheme + authority.
      return new DefaultCredScopedKey(uri, conf);
    }
  }

  class PathCredScopedKey implements CredScopedKey {
    private final String path;
    private final String pathOperation;

    public PathCredScopedKey(String path, String pathOperation) {
      this.path = path;
      this.pathOperation = pathOperation;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof PathCredScopedKey)) return false;
      PathCredScopedKey that = (PathCredScopedKey) o;
      return Objects.equals(path, that.path) && Objects.equals(pathOperation, that.pathOperation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(path, pathOperation);
    }

    @Override
    public String toString() {
      return "PathCredScopedKey{path=" + path + ", op=" + pathOperation + "}";
    }
  }

  class TableCredScopedKey implements CredScopedKey {
    private final String tableId;
    private final String tableOperation;

    public TableCredScopedKey(String tableId, String tableOperation) {
      this.tableId = tableId;
      this.tableOperation = tableOperation;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TableCredScopedKey)) return false;
      TableCredScopedKey that = (TableCredScopedKey) o;
      return Objects.equals(tableId, that.tableId)
          && Objects.equals(tableOperation, that.tableOperation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableId, tableOperation);
    }

    @Override
    public String toString() {
      return "TableCredScopedKey{tableId=" + tableId + ", op=" + tableOperation + "}";
    }
  }

  class DeltaTableCredScopedKey implements CredScopedKey {
    private final UCDeltaTableIdentifier identifier;
    private final String tableOperation;
    private final String location;

    public DeltaTableCredScopedKey(
        UCDeltaTableIdentifier identifier, String tableOperation, String location) {
      this.identifier = identifier;
      this.tableOperation = tableOperation;
      this.location = location;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof DeltaTableCredScopedKey)) return false;
      DeltaTableCredScopedKey that = (DeltaTableCredScopedKey) o;
      return Objects.equals(identifier, that.identifier)
          && Objects.equals(tableOperation, that.tableOperation)
          && Objects.equals(location, that.location);
    }

    @Override
    public int hashCode() {
      return Objects.hash(identifier, tableOperation, location);
    }

    @Override
    public String toString() {
      return "DeltaTableCredScopedKey{table="
          + identifier
          + ", op="
          + tableOperation
          + ", location="
          + location
          + "}";
    }
  }

  class DeltaStagingTableCredScopedKey implements CredScopedKey {
    private final String stagingTableId;
    private final String location;

    public DeltaStagingTableCredScopedKey(String stagingTableId, String location) {
      this.stagingTableId = stagingTableId;
      this.location = location;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof DeltaStagingTableCredScopedKey)) return false;
      DeltaStagingTableCredScopedKey that = (DeltaStagingTableCredScopedKey) o;
      return Objects.equals(stagingTableId, that.stagingTableId)
          && Objects.equals(location, that.location);
    }

    @Override
    public int hashCode() {
      return Objects.hash(stagingTableId, location);
    }

    @Override
    public String toString() {
      return "DeltaStagingTableCredScopedKey{stagingTableId="
          + stagingTableId
          + ", location="
          + location
          + "}";
    }
  }

  class DefaultCredScopedKey implements CredScopedKey {
    private final String scheme;
    private final String authority;

    public DefaultCredScopedKey(URI uri, Configuration conf) {
      if (uri.getScheme() == null && uri.getAuthority() == null) {
        URI defaultUri = FileSystem.getDefaultUri(conf);
        this.scheme = defaultUri.getScheme();
        this.authority = defaultUri.getAuthority();
      } else {
        this.scheme = uri.getScheme();
        this.authority = uri.getAuthority();
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof DefaultCredScopedKey)) return false;
      DefaultCredScopedKey that = (DefaultCredScopedKey) o;
      return Objects.equals(scheme, that.scheme) && Objects.equals(authority, that.authority);
    }

    @Override
    public int hashCode() {
      return Objects.hash(scheme, authority);
    }

    @Override
    public String toString() {
      return "DefaultCredScopedKey{scheme=" + scheme + ", authority=" + authority + "}";
    }
  }
}
