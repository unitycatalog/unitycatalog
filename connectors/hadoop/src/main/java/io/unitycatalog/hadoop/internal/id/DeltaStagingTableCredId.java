package io.unitycatalog.hadoop.internal.id;

import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Map;
import java.util.Objects;

/**
 * {@link CredId} keyed by credential context, staging table ID, and location; used for
 * staging-table-level temporary credentials via the UC Delta credentials API.
 */
public class DeltaStagingTableCredId implements CredId {
  private final String credContextId;
  private final String stagingTableId;
  private final String location;

  public DeltaStagingTableCredId(String credContextId, String stagingTableId, String location) {
    Preconditions.checkNotNull(credContextId, "credContextId is required");
    Preconditions.checkNotNull(stagingTableId, "stagingTableId is required");
    Preconditions.checkNotNull(location, "location is required");
    this.credContextId = credContextId;
    this.stagingTableId = stagingTableId;
    this.location = location;
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
        UC_CRED_CONTEXT_ID_KEY, credContextId,
        UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true",
        UC_DELTA_STAGING_TABLE_ID_KEY, stagingTableId,
        UC_DELTA_STAGING_TABLE_LOCATION_KEY, location);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DeltaStagingTableCredId)) return false;
    DeltaStagingTableCredId that = (DeltaStagingTableCredId) o;
    return Objects.equals(credContextId, that.credContextId)
        && Objects.equals(stagingTableId, that.stagingTableId)
        && Objects.equals(location, that.location);
  }

  @Override
  public int hashCode() {
    return Objects.hash(credContextId, stagingTableId, location);
  }

  @Override
  public String toString() {
    return "DeltaStagingTableCredId{credContextId="
        + credContextId
        + ", stagingTableId="
        + stagingTableId
        + ", location="
        + location
        + "}";
  }
}
