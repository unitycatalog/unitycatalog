package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.DependencyList;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.DependencyRepository;
import io.unitycatalog.server.persist.PropertyRepository;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.DependencyDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.utils.Constants;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.LockMode;
import org.hibernate.Session;
import org.hibernate.query.Query;

public class RepositoryUtils {

  private static final Map<String, Class<?>> PROPERTY_TYPE_MAP = new HashMap<>();

  /**
   * Acquires the table row lock shared by the Delta and Iceberg commit paths and by table deletion,
   * so concurrent commits on the same table serialize, and a deletion waits for them. A lock-wait
   * timeout or deadlock victim is reported as {@code lockContentionError} (rolling back only this
   * transaction) rather than leaking the ORM exception; each caller passes the code whose client
   * reaction is safe for it:
   *
   * <ul>
   *   <li><b>Delta</b> passes {@code COMMIT_STATE_UNKNOWN} (500). Failing to acquire the lock does
   *       not reveal whether the logical commit landed: the lock holder may be this same commit's
   *       own earlier attempt (a slow client retry carrying the same UUID) that goes on to succeed.
   *       A conflict would let the client conclude it did not land and rebase, committing the same
   *       change twice. Delta commits are deduplicated by that UUID (or by content), so the client
   *       instead resends the identical request and the server resolves it to the true outcome.
   *   <li><b>Iceberg</b> passes {@code UPDATE_REQUIREMENT_CONFLICT} (409). An Iceberg commit is a
   *       compare-and-swap on the expected metadata location, so a retry after an earlier attempt
   *       landed fails that check and the client rebuilds; it cannot double-commit. A 409 is
   *       therefore safe and lets the Iceberg client retry transparently, whereas a 500 would
   *       surface to the user. Iceberg has no server-side deduplication that a resend could
   *       exploit.
   *   <li><b>Table deletion</b> passes {@code UNAVAILABLE} (503): the table was not deleted, and
   *       the request can be retried. The Iceberg REST spec lists 503, not 409, for drop table, and
   *       the Iceberg client reads a 409 on drop as "already exists".
   * </ul>
   *
   * <p>The row is locked first and read second. Refreshing the entity under the lock would load its
   * columns with an outer join, and on PostgreSQL Hibernate locks such a query in a separate
   * statement after the read, so a commit that waited for the lock would continue on the state it
   * read before the other commit landed. The re-read is a locking read as well: on MySQL's default
   * REPEATABLE READ, a plain read returns the snapshot the transaction took at its first read,
   * which can be from before the other commit. A table deleted while this waited for the lock is
   * reported as {@link ErrorCode#TABLE_NOT_FOUND}.
   *
   * @param session the Hibernate session running the commit or deletion transaction
   * @param dao the table row to lock; re-read after the lock is held
   * @param tableId the table id, used as a fallback in the error message
   * @param tableFullNameForLogging the table's full name for the error message, if known
   * @param lockContentionError the error code to raise when the lock cannot be acquired
   */
  public static void lockTableForCommit(
      Session session,
      TableInfoDAO dao,
      UUID tableId,
      Optional<String> tableFullNameForLogging,
      ErrorCode lockContentionError) {
    String tableName = tableFullNameForLogging.orElseGet(tableId::toString);
    try {
      TableInfoDAO locked =
          session
              .createSelectionQuery("from TableInfoDAO where id = :id", TableInfoDAO.class)
              .setParameter("id", dao.getId())
              .setHibernateLockMode(LockMode.PESSIMISTIC_WRITE)
              .getSingleResultOrNull();
      if (locked == null) {
        throw new BaseException(ErrorCode.TABLE_NOT_FOUND, "Table not found: " + tableName);
      }
      session.refresh(dao, LockMode.PESSIMISTIC_WRITE);
    } catch (RuntimeException e) {
      // The lock query wraps Hibernate's lock exception in a JPA one, so check the cause as well.
      // A deadlock victim arrives as OptimisticLockException wrapping LockAcquisitionException.
      if (!isLockFailure(e) && !isLockFailure(e.getCause())) {
        throw e;
      }
      throw new BaseException(
          lockContentionError,
          "Concurrent modification in progress on table " + tableName + "; retry the request.");
    }
  }

  /**
   * Whether {@code failure} reports that a lock was not acquired: a lock-wait failure, a lock
   * timeout, or a deadlock victim. {@code LockTimeoutException} is a {@code
   * LockAcquisitionException}.
   */
  private static boolean isLockFailure(Throwable failure) {
    return failure instanceof jakarta.persistence.PessimisticLockException
        || failure instanceof jakarta.persistence.LockTimeoutException
        || failure instanceof org.hibernate.PessimisticLockException
        || failure instanceof org.hibernate.exception.LockAcquisitionException;
  }

  static {
    PROPERTY_TYPE_MAP.put(Constants.FUNCTION, String.class);
  }

  public static <T> T attachProperties(
      T entityInfo, String uuid, String entityType, Session session) {
    try {
      List<PropertyDAO> propertyDAOList =
          PropertyRepository.findProperties(session, UUID.fromString(uuid), entityType);
      if (propertyDAOList.isEmpty()) {
        return entityInfo;
      }
      Class<?> entityClass = PROPERTY_TYPE_MAP.getOrDefault(entityType, Map.class);
      Method setPropertiesMethod = entityInfo.getClass().getMethod("setProperties", entityClass);
      Map<String, String> propertyMap = PropertyDAO.toMap(propertyDAOList);
      Object propertiesArgument =
          switch (entityClass.getSimpleName()) {
            case "Map" -> propertyMap;
            case "String" -> propertyMap.toString();
            default ->
                throw new IllegalArgumentException(
                    "Unsupported parameter type: " + entityClass.getSimpleName());
          };
      setPropertiesMethod.invoke(entityInfo, propertiesArgument);
      return entityInfo;
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean isViewLike(String tableTypeValue) {
    return TableType.METRIC_VIEW.getValue().equals(tableTypeValue)
        || TableType.VIEW.getValue().equals(tableTypeValue);
  }

  public static void attachDependencies(
      TableInfo tableInfo,
      TableInfoDAO tableInfoDAO,
      Session session,
      DependencyRepository dependencyRepository) {
    if (isViewLike(tableInfoDAO.getType())) {
      List<DependencyDAO> deps =
          dependencyRepository.getDependencies(
              session, tableInfoDAO.getId(), DependencyDAO.DependentType.TABLE);
      tableInfo.setViewDependencies(
          new DependencyList().dependencies(DependencyDAO.toDependencyList(deps)));
    }
  }

  /**
   * Splits a dotted full name into exactly {@code expectedParts} non-empty parts. Throws {@code
   * INVALID_ARGUMENT} with {@code errorMessage} otherwise, including when any part is empty (such
   * as {@code a..b} or a trailing dot).
   */
  public static String[] splitFullName(String fullName, int expectedParts, String errorMessage) {
    String[] parts = fullName.split("\\.", -1);
    if (parts.length != expectedParts || Arrays.stream(parts).anyMatch(String::isEmpty)) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, errorMessage);
    }
    return parts;
  }

  public static String[] parseFullName(String fullName) {
    return splitFullName(fullName, 3, "Invalid registered model name: " + fullName);
  }

  public static String getAssetFullName(String catalogName, String schemaName, String assetName) {
    return catalogName + "." + schemaName + "." + assetName;
  }

  public static Optional<CatalogInfoDAO> getCatalogDaoOpt(Session session, String name) {
    Query<CatalogInfoDAO> query =
        session.createQuery("FROM CatalogInfoDAO WHERE name = :value", CatalogInfoDAO.class);
    query.setParameter("value", name);
    query.setMaxResults(1);
    return query.uniqueResultOptional();
  }

  public static Optional<SchemaInfoDAO> getSchemaDaoOpt(
      Session session, UUID catalogId, String schemaName) {
    Query<SchemaInfoDAO> query =
        session.createQuery(
            "FROM SchemaInfoDAO WHERE name = :name and catalogId = :catalogId",
            SchemaInfoDAO.class);
    query.setParameter("name", schemaName);
    query.setParameter("catalogId", catalogId);
    query.setMaxResults(1);
    return query.uniqueResultOptional();
  }

  public record CatalogAndSchemaDaoOpt(
      Optional<CatalogInfoDAO> catalogInfoDAO, Optional<SchemaInfoDAO> schemaInfoDAO) {}

  public record CatalogAndSchemaDao(CatalogInfoDAO catalogInfoDAO, SchemaInfoDAO schemaInfoDAO) {}

  public static CatalogAndSchemaDaoOpt getCatalogAndSchemaDaoOpt(
      Session session, String catalogName, String schemaName) {
    Optional<CatalogInfoDAO> catalog = getCatalogDaoOpt(session, catalogName);
    if (catalog.isEmpty()) {
      return new CatalogAndSchemaDaoOpt(Optional.empty(), Optional.empty());
    }
    Optional<SchemaInfoDAO> schema = getSchemaDaoOpt(session, catalog.get().getId(), schemaName);
    return new CatalogAndSchemaDaoOpt(catalog, schema);
  }

  public static CatalogAndSchemaDao getCatalogAndSchemaDaoOrThrow(
      Session session, String catalogName, String schemaName) {
    CatalogAndSchemaDaoOpt catalogAndSchemaDaoOpt =
        getCatalogAndSchemaDaoOpt(session, catalogName, schemaName);
    return new CatalogAndSchemaDao(
        catalogAndSchemaDaoOpt
            .catalogInfoDAO()
            .orElseThrow(
                () ->
                    new BaseException(
                        ErrorCode.CATALOG_NOT_FOUND, "Catalog not found: " + catalogName)),
        catalogAndSchemaDaoOpt
            .schemaInfoDAO()
            .orElseThrow(
                () ->
                    new BaseException(
                        ErrorCode.SCHEMA_NOT_FOUND,
                        "Schema not found: " + catalogName + "." + schemaName)));
  }

  public record CatalogAndSchemaNames(String catalogName, String schemaName) {}

  /**
   * Retrieves the catalog and schema names for a given schema ID.
   *
   * <p>This method performs a lookup to find the schema by its UUID, then retrieves the associated
   * catalog information. It returns both the catalog and schema names as a pair.
   *
   * @param session the Hibernate session used to query the database
   * @param schemaId the unique identifier of the schema
   * @return a CatalogAndSchemaNames record
   * @throws BaseException with ErrorCode.SCHEMA_NOT_FOUND or ErrorCode.CATALOG_NOT_FOUND if the
   *     schema or its parent catalog is not found
   */
  public static CatalogAndSchemaNames getCatalogAndSchemaNames(Session session, UUID schemaId) {
    SchemaInfoDAO schemaInfoDAO = session.get(SchemaInfoDAO.class, schemaId);
    if (schemaInfoDAO == null) {
      throw new BaseException(ErrorCode.SCHEMA_NOT_FOUND, "Schema not found: " + schemaId);
    }
    CatalogInfoDAO catalogInfoDAO = session.get(CatalogInfoDAO.class, schemaInfoDAO.getCatalogId());
    if (catalogInfoDAO == null) {
      throw new BaseException(
          ErrorCode.CATALOG_NOT_FOUND, "Catalog not found: " + schemaInfoDAO.getCatalogId());
    }
    return new CatalogAndSchemaNames(catalogInfoDAO.getName(), schemaInfoDAO.getName());
  }
}
