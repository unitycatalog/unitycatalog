package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.delta.model.DeltaTableType;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.TableType;
import java.util.UUID;

/**
 * Two-way mapping between the Delta API {@link DeltaTableType} and how a table is persisted: the
 * stored UC {@link TableType} plus the {@code base_table_id} column.
 *
 * <p>Shallow clones are persisted as plain MANAGED / EXTERNAL rows with a non-null {@code
 * base_table_id}, so every stored-type-keyed code path applies to clones unchanged; the {@code
 * *_SHALLOW_CLONE} variants exist only on the Delta API wire. Both directions live here so the
 * mapping cannot drift between the create path ({@link DeltaCreateTableMapper}) and the response
 * builder ({@code TableRepository}).
 */
public final class DeltaTableTypes {

  private DeltaTableTypes() {}

  /** Whether {@code type} is one of the shallow-clone variants. */
  public static boolean isShallowClone(DeltaTableType type) {
    return type == DeltaTableType.MANAGED_SHALLOW_CLONE
        || type == DeltaTableType.EXTERNAL_SHALLOW_CLONE;
  }

  /**
   * The stored UC table type for a requested Delta table type. Shallow-clone variants collapse
   * onto their non-clone counterpart; the clone relationship is persisted separately as {@code
   * base_table_id}.
   *
   * @throws BaseException UNIMPLEMENTED for {@link DeltaTableType#EXTERNAL_SHALLOW_CLONE}, which
   *     the spec reserves but the server does not support yet.
   */
  public static TableType toStoredTableType(DeltaTableType type) {
    return switch (type) {
      case MANAGED, MANAGED_SHALLOW_CLONE -> TableType.MANAGED;
      case EXTERNAL -> TableType.EXTERNAL;
      case EXTERNAL_SHALLOW_CLONE ->
          throw new BaseException(
              ErrorCode.UNIMPLEMENTED, "EXTERNAL_SHALLOW_CLONE tables are not supported yet.");
    };
  }

  /**
   * The Delta API table type for a stored row: the stored type string promoted to its {@code
   * *_SHALLOW_CLONE} variant when the row carries a {@code base_table_id}.
   */
  public static DeltaTableType fromStored(String storedType, UUID baseTableId) {
    DeltaTableType type = DeltaTableType.fromValue(storedType);
    if (baseTableId == null) {
      return type;
    }
    return switch (type) {
      case MANAGED -> DeltaTableType.MANAGED_SHALLOW_CLONE;
      case EXTERNAL -> DeltaTableType.EXTERNAL_SHALLOW_CLONE;
      default ->
          throw new BaseException(
              ErrorCode.INTERNAL,
              "Stored table type " + storedType + " cannot carry a base_table_id.");
    };
  }
}
