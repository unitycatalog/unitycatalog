package io.unitycatalog.hadoop.internal.id;

import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_AUTH_UNIQUE_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_CATALOG_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_LOCATION_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_TABLE_OPERATION_KEY;

import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.UCDeltaTableIdentifier;
import java.util.Map;
import java.util.Objects;

/**
 * {@link CredId} keyed by auth config, table identity, operation, and location; used for
 * table-level temporary credentials via the UC Delta credentials API.
 */
public class DeltaTableCredId implements CredId {
  private final String authUniqueId;
  private final UCDeltaTableIdentifier identifier;
  private final String tableOperation;
  private final String location;

  public DeltaTableCredId(
      String authUniqueId,
      UCDeltaTableIdentifier identifier,
      String tableOperation,
      String location) {
    Preconditions.checkNotNull(authUniqueId, "authUniqueId is required");
    Preconditions.checkNotNull(identifier, "identifier is required");
    Preconditions.checkNotNull(tableOperation, "tableOperation is required");
    Preconditions.checkNotNull(location, "location is required");
    this.authUniqueId = authUniqueId;
    this.identifier = identifier;
    this.tableOperation = tableOperation;
    this.location = location;
  }

  public String authUniqueId() {
    return authUniqueId;
  }

  public UCDeltaTableIdentifier identifier() {
    return identifier;
  }

  public String tableOperation() {
    return tableOperation;
  }

  public String location() {
    return location;
  }

  @Override
  public Map<String, String> props() {
    return Map.of(
        UC_AUTH_UNIQUE_ID_KEY,
        authUniqueId,
        UC_DELTA_CREDENTIALS_API_ENABLED_KEY,
        "true",
        UC_CREDENTIALS_TYPE_KEY,
        UC_CREDENTIALS_TYPE_TABLE_VALUE,
        UC_DELTA_CATALOG_KEY,
        identifier.catalog(),
        UC_DELTA_SCHEMA_KEY,
        identifier.schema(),
        UC_DELTA_TABLE_NAME_KEY,
        identifier.table(),
        UC_DELTA_LOCATION_KEY,
        location,
        UC_TABLE_OPERATION_KEY,
        tableOperation);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DeltaTableCredId)) return false;
    DeltaTableCredId that = (DeltaTableCredId) o;
    return Objects.equals(authUniqueId, that.authUniqueId)
        && Objects.equals(identifier, that.identifier)
        && Objects.equals(tableOperation, that.tableOperation)
        && Objects.equals(location, that.location);
  }

  @Override
  public int hashCode() {
    return Objects.hash(authUniqueId, identifier, tableOperation, location);
  }

  @Override
  public String toString() {
    return "DeltaTableCredId{authUniqueId="
        + authUniqueId
        + ", table="
        + identifier
        + ", op="
        + tableOperation
        + ", location="
        + location
        + "}";
  }
}
