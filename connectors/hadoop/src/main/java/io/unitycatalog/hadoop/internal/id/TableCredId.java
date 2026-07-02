package io.unitycatalog.hadoop.internal.id;

import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_TABLE_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_TABLE_OPERATION_KEY;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Map;
import java.util.Objects;

/**
 * {@link CredId} keyed by table ID and operation; used for table-level temporary credentials via
 * the UC credentials API.
 */
public class TableCredId implements CredId {
  private final String tableId;
  private final String tableOperation;

  public TableCredId(String tableId, String tableOperation) {
    Preconditions.checkNotNull(tableId, "tableId is required");
    Preconditions.checkNotNull(tableOperation, "tableOperation is required");
    this.tableId = tableId;
    this.tableOperation = tableOperation;
  }

  public String tableId() {
    return tableId;
  }

  public String tableOperation() {
    return tableOperation;
  }

  @Override
  public Map<String, String> props() {
    return Map.of(
        UC_CREDENTIALS_TYPE_KEY, UC_CREDENTIALS_TYPE_TABLE_VALUE,
        UC_TABLE_ID_KEY, tableId,
        UC_TABLE_OPERATION_KEY, tableOperation);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TableCredId)) return false;
    TableCredId that = (TableCredId) o;
    return Objects.equals(tableId, that.tableId)
        && Objects.equals(tableOperation, that.tableOperation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableId, tableOperation);
  }

  @Override
  public String toString() {
    return "TableCredId{tableId=" + tableId + ", op=" + tableOperation + "}";
  }
}
