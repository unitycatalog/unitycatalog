package io.unitycatalog.spark.fs;

import io.unitycatalog.hadoop.internal.UCHadoopConf;
import java.net.URI;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * Cache key that identifies a credential scope for {@link CredScopedFileSystem}.
 *
 * <p>There are three implementations:
 *
 * <ul>
 *   <li>{@link TableCredScopedKey} — keyed by table ID and operation; used for table-level
 *       temporary credentials.
 *   <li>{@link PathCredScopedKey} — keyed by path and operation; used for path-level temporary
 *       credentials.
 *   <li>{@link DefaultCredScopedKey} — keyed by URI scheme and authority; used as a fallback when
 *       no Unity Catalog credential type is present in the configuration.
 * </ul>
 */
public interface CredScopedKey {

  static CredScopedKey create(URI uri, Configuration conf) {
    String type = conf.get(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY);
    if (UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(type)) {
      String path = conf.get(UCHadoopConf.UC_PATH_KEY);
      String pathOperation = conf.get(UCHadoopConf.UC_PATH_OPERATION_KEY);

      return new PathCredScopedKey(path, pathOperation);
    } else if (UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type)) {
      String tableId = conf.get(UCHadoopConf.UC_TABLE_ID_KEY);
      String tableOperation = conf.get(UCHadoopConf.UC_TABLE_OPERATION_KEY);
      return new TableCredScopedKey(tableId, tableOperation);
    }

    return new DefaultCredScopedKey(uri, conf);
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
