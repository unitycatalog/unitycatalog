package io.unitycatalog.spark.auth;

import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import java.util.Objects;

public interface GenericCredKey {

  class TableCredKey implements GenericCredKey {
    private final String uri;
    private final String token;
    private final String tableId;
    private final TableOperation tableOp;

    public TableCredKey(String uri, String token, String tableId, TableOperation tableOp) {
      this.uri = uri;
      this.token = token;
      this.tableId = tableId;
      this.tableOp = tableOp;
    }

    public String tableId() {
      return tableId;
    }

    public TableOperation tableOp() {
      return tableOp;
    }

    @Override
    public int hashCode() {
      return Objects.hash(uri, token, tableId, tableOp);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TableCredKey that = (TableCredKey) o;
      return Objects.equals(uri, that.uri)
          && Objects.equals(token, that.token)
          && Objects.equals(tableId, that.tableId)
          && Objects.equals(tableOp, that.tableOp);
    }
  }

  class PathCredKey implements GenericCredKey {
    private final String uri;
    private final String token;
    private final String path;
    private final PathOperation pathOp;

    public PathCredKey(String uri, String token, String path, PathOperation pathOp) {
      this.uri = uri;
      this.token = token;
      this.path = path;
      this.pathOp = pathOp;
    }

    public String path() {
      return path;
    }

    public PathOperation pathOp() {
      return pathOp;
    }

    @Override
    public int hashCode() {
      return Objects.hash(uri, token, path, pathOp);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      PathCredKey that = (PathCredKey) o;
      return Objects.equals(uri, that.uri)
          && Objects.equals(token, that.token)
          && Objects.equals(path, that.path)
          && Objects.equals(pathOp, that.pathOp);
    }
  }
}
