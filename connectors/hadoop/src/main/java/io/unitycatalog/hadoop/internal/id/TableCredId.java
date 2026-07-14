package io.unitycatalog.hadoop.internal.id;

import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_TABLE_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_TABLE_OPERATION_KEY;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Map;
import java.util.Objects;

/**
 * {@link CredId} keyed by credential context, table ID, and operation; used for table-level
 * temporary credentials via the UC credentials API.
 */
public class TableCredId implements CredId {
  private final String credContextId;
  private final String tableId;
  private final String tableOperation;

  public TableCredId(String credContextId, String tableId, String tableOperation) {
    Preconditions.checkNotNull(credContextId, "credContextId is required");
    Preconditions.checkNotNull(tableId, "tableId is required");
    Preconditions.checkNotNull(tableOperation, "tableOperation is required");
    this.credContextId = credContextId;
    this.tableId = tableId;
    this.tableOperation = tableOperation;
  }

  public String credContextId() {
    return credContextId;
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
        UC_CRED_CONTEXT_ID_KEY, credContextId,
        UC_CREDENTIALS_TYPE_KEY, UC_CREDENTIALS_TYPE_TABLE_VALUE,
        UC_TABLE_ID_KEY, tableId,
        UC_TABLE_OPERATION_KEY, tableOperation);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TableCredId)) return false;
    TableCredId that = (TableCredId) o;
    return Objects.equals(credContextId, that.credContextId)
        && Objects.equals(tableId, that.tableId)
        && Objects.equals(tableOperation, that.tableOperation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(credContextId, tableId, tableOperation);
  }

  @Override
  public String toString() {
    return "TableCredId{credContextId="
        + credContextId
        + ", tableId="
        + tableId
        + ", op="
        + tableOperation
        + "}";
  }
}
