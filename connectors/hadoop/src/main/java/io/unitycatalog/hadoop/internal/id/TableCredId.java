package io.unitycatalog.hadoop.internal.id;

import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_TABLE_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_TABLE_OPERATION_KEY;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Collections;
import java.util.HashMap;
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
    this.tableId = Preconditions.checkNotNull(tableId, "tableId is required");
    this.tableOperation = Preconditions.checkNotNull(tableOperation, "tableOperation is required");
  }

  public String tableId() {
    return tableId;
  }

  public String tableOperation() {
    return tableOperation;
  }

  @Override
  public Map<String, String> props() {
    Map<String, String> props = new HashMap<>();
    props.put(UC_CREDENTIALS_TYPE_KEY, UC_CREDENTIALS_TYPE_TABLE_VALUE);
    props.put(UC_TABLE_ID_KEY, tableId);
    props.put(UC_TABLE_OPERATION_KEY, tableOperation);
    return Collections.unmodifiableMap(props);
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
