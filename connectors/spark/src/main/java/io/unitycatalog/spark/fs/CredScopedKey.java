package io.unitycatalog.spark.fs;

import io.unitycatalog.spark.UCHadoopConf;
import java.net.URI;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

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

    return new NoopCredScopedKey(uri, conf);
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

  class NoopCredScopedKey implements CredScopedKey {
    private final String scheme;
    private final String authority;

    public NoopCredScopedKey(URI uri, Configuration conf) {
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
      if (!(o instanceof NoopCredScopedKey)) return false;
      NoopCredScopedKey that = (NoopCredScopedKey) o;
      return Objects.equals(scheme, that.scheme) && Objects.equals(authority, that.authority);
    }

    @Override
    public int hashCode() {
      return Objects.hash(scheme, authority);
    }

    @Override
    public String toString() {
      return "NoopCredScopedKey{scheme=" + scheme + ", authority=" + authority + "}";
    }
  }
}
