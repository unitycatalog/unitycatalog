package io.unitycatalog.hadoop.internal.id;

import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_AUTH_UNIQUE_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_PATH_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_PATH_OPERATION_KEY;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Map;
import java.util.Objects;

/**
 * {@link CredId} keyed by auth config, path, and operation; used for path-level temporary
 * credentials.
 */
public class PathCredId implements CredId {
  private final String authUniqueId;
  private final String path;
  private final String pathOperation;

  public PathCredId(String authUniqueId, String path, String pathOperation) {
    Preconditions.checkNotNull(authUniqueId, "authUniqueId is required");
    Preconditions.checkNotNull(path, "path is required");
    Preconditions.checkNotNull(pathOperation, "pathOperation is required");
    this.authUniqueId = authUniqueId;
    this.path = path;
    this.pathOperation = pathOperation;
  }

  public String authUniqueId() {
    return authUniqueId;
  }

  public String path() {
    return path;
  }

  public String pathOperation() {
    return pathOperation;
  }

  @Override
  public Map<String, String> props() {
    return Map.of(
        UC_AUTH_UNIQUE_ID_KEY, authUniqueId,
        UC_CREDENTIALS_TYPE_KEY, UC_CREDENTIALS_TYPE_PATH_VALUE,
        UC_PATH_KEY, path,
        UC_PATH_OPERATION_KEY, pathOperation);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PathCredId)) return false;
    PathCredId that = (PathCredId) o;
    return Objects.equals(authUniqueId, that.authUniqueId)
        && Objects.equals(path, that.path)
        && Objects.equals(pathOperation, that.pathOperation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(authUniqueId, path, pathOperation);
  }

  @Override
  public String toString() {
    return "PathCredId{authUniqueId="
        + authUniqueId
        + ", path="
        + path
        + ", op="
        + pathOperation
        + "}";
  }
}
