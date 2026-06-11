package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import java.util.List;
import java.util.UUID;
import org.hibernate.Session;

/** Shallow-clone validation helpers shared by the table create and delete paths. */
public final class ShallowCloneUtils {

  private ShallowCloneUtils() {}

  /** How many clone names to include in the drop-protection error message. */
  private static final int MAX_CLONE_NAMES_IN_ERROR = 5;

  /**
   * Validate the base table of a shallow clone: it must exist, be a Delta table, have the same
   * table type as the clone -- a clone never crosses the managed/external boundary, so a MANAGED
   * shallow clone requires a MANAGED base (and EXTERNAL_SHALLOW_CLONE, once supported, an EXTERNAL
   * base) -- and must not itself be a shallow clone. A self-referencing clone is impossible because
   * the new table's UUID belongs to a staging row, not a table row.
   *
   * @param cloneTableType the clone's stored table type (MANAGED for MANAGED_SHALLOW_CLONE)
   */
  public static void validateBaseTable(
      Session session, UUID baseTableId, TableType cloneTableType) {
    TableInfoDAO base = session.get(TableInfoDAO.class, baseTableId);
    if (base == null) {
      throw new BaseException(
          ErrorCode.TABLE_NOT_FOUND, "Base table not found with id: " + baseTableId);
    }
    if (!cloneTableType.toString().equals(base.getType())
        || !DataSourceFormat.DELTA.toString().equals(base.getDataSourceFormat())) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Base table of a "
              + cloneTableType
              + " shallow clone must be a "
              + cloneTableType
              + " Delta table: "
              + baseTableId);
    }
    if (base.getBaseTableId() != null) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Base table "
              + baseTableId
              + " is itself a shallow clone; cloning a clone is not supported. Clone its base"
              + " table "
              + base.getBaseTableId()
              + " instead.");
    }
  }

  /** Drop protection: block deleting a table that is still the base of shallow clones */
  public static void validateNoActiveClones(Session session, TableInfoDAO table) {
    List<String> cloneNames =
        session
            .createQuery(
                "SELECT t.name FROM TableInfoDAO t WHERE t.baseTableId = :baseId ORDER BY t.name",
                String.class)
            .setParameter("baseId", table.getId())
            .setMaxResults(MAX_CLONE_NAMES_IN_ERROR)
            .getResultList();
    if (!cloneNames.isEmpty()) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "Cannot delete table "
              + table.getName()
              + ": it is the base table of shallow clone(s) "
              + cloneNames
              + ". Delete the clones first.");
    }
  }
}
