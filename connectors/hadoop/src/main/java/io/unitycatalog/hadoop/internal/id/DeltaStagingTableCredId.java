package io.unitycatalog.hadoop.internal.id;

import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_AUTH_UNIQUE_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Map;
import java.util.Objects;

/**
 * {@link CredId} keyed by auth config, staging table ID, and location; used for staging-table-level
 * temporary credentials via the UC Delta credentials API.
 */
public class DeltaStagingTableCredId implements CredId {
  private final String authUniqueId;
  private final String stagingTableId;
  private final String location;

  public DeltaStagingTableCredId(String authUniqueId, String stagingTableId, String location) {
    Preconditions.checkNotNull(authUniqueId, "authUniqueId is required");
    Preconditions.checkNotNull(stagingTableId, "stagingTableId is required");
    Preconditions.checkNotNull(location, "location is required");
    this.authUniqueId = authUniqueId;
    this.stagingTableId = stagingTableId;
    this.location = location;
  }

  public String authUniqueId() {
    return authUniqueId;
  }

  public String stagingTableId() {
    return stagingTableId;
  }

  public String location() {
    return location;
  }

  @Override
  public Map<String, String> props() {
    return Map.of(
        UC_AUTH_UNIQUE_ID_KEY, authUniqueId,
        UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true",
        UC_DELTA_STAGING_TABLE_ID_KEY, stagingTableId,
        UC_DELTA_STAGING_TABLE_LOCATION_KEY, location);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DeltaStagingTableCredId)) return false;
    DeltaStagingTableCredId that = (DeltaStagingTableCredId) o;
    return Objects.equals(authUniqueId, that.authUniqueId)
        && Objects.equals(stagingTableId, that.stagingTableId)
        && Objects.equals(location, that.location);
  }

  @Override
  public int hashCode() {
    return Objects.hash(authUniqueId, stagingTableId, location);
  }

  @Override
  public String toString() {
    return "DeltaStagingTableCredId{authUniqueId="
        + authUniqueId
        + ", stagingTableId="
        + stagingTableId
        + ", location="
        + location
        + "}";
  }
}
