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
   * Acquires the table row lock shared by the Delta and Iceberg commit paths, so concurrent commits
   * on the same table serialize. A lock-wait timeout or deadlock victim is reported as {@code
   * lockContentionError} (rolling back only this transaction) rather than leaking the ORM
   * exception; the two commit paths pass different codes because the safe client reaction differs:
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
   * </ul>
   *
   * @param session the Hibernate session running the commit transaction
   * @param dao the table row to lock; refreshed under {@code PESSIMISTIC_WRITE}
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
    try {
      session.refresh(dao, LockMode.PESSIMISTIC_WRITE);
    } catch (RuntimeException e) {
      if (!(e instanceof org.hibernate.PessimisticLockException)
          && !(e instanceof org.hibernate.exception.LockAcquisitionException)
          && !(e instanceof org.hibernate.exception.LockTimeoutException)) {
        throw e;
      }
      throw new BaseException(
          lockContentionError,
          "Concurrent commit in progress on table "
              + tableFullNameForLogging.orElseGet(tableId::toString)
              + "; retry the request.");
    }
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

  public static String[] parseFullName(String fullName) {
    String[] parts = fullName.split("\\.");
    if (parts.length != 3) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Invalid registered model name: " + fullName);
    }
    return parts;
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
