package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.PropertyRepository;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.utils.Constants;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.HashMap;
import org.hibernate.Session;
import org.hibernate.query.Query;

public class RepositoryUtils {

  private static final Map<String, Class<?>> PROPERTY_TYPE_MAP = new HashMap<>();

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
      Method setPropertiesMethod =
          entityInfo.getClass().getMethod("setProperties", entityClass);
      Map<String, String> propertyMap = PropertyDAO.toMap(propertyDAOList);
      Object propertiesArgument = switch (entityClass.getSimpleName()) {
        case "Map" -> propertyMap;
        case "String" -> propertyMap.toString();
        default -> throw new IllegalArgumentException(
            "Unsupported parameter type: " + entityClass.getSimpleName());
      };
      setPropertiesMethod.invoke(entityInfo, propertiesArgument);
      return entityInfo;
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
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

  public static String getAssetFullName(
      String catalogName, String schemaName, String assetName) {
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
  public record CatalogAndSchemaDao(
      CatalogInfoDAO catalogInfoDAO, SchemaInfoDAO schemaInfoDAO) {}

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
                () -> new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + catalogName)),
        catalogAndSchemaDaoOpt
            .schemaInfoDAO()
            .orElseThrow(
                () ->
                    new BaseException(
                        ErrorCode.NOT_FOUND,
                        "Schema not found: " + catalogName + "." + schemaName)));
  }

  public record CatalogAndSchemaNames(String catalogName, String schemaName) {}

  /**
   * Retrieves the catalog and schema names for a given schema ID.
   *
   * <p>This method performs a lookup to find the schema by its UUID, then retrieves
   * the associated catalog information. It returns both the catalog and schema names
   * as a pair.
   *
   * @param session the Hibernate session used to query the database
   * @param schemaId the unique identifier of the schema
   * @return a CatalogAndSchemaNames record
   * @throws BaseException with ErrorCode.NOT_FOUND if the schema or its parent catalog is not found
   */
  public static CatalogAndSchemaNames getCatalogAndSchemaNames(Session session, UUID schemaId) {
    SchemaInfoDAO schemaInfoDAO = session.get(SchemaInfoDAO.class, schemaId);
    if (schemaInfoDAO == null) {
      throw new BaseException(
              ErrorCode.NOT_FOUND, "Schema not found: " + schemaId);
    }
    CatalogInfoDAO catalogInfoDAO = session.get(CatalogInfoDAO.class, schemaInfoDAO.getCatalogId());
    if (catalogInfoDAO == null) {
      throw new BaseException(
              ErrorCode.NOT_FOUND, "Catalog not found: " + schemaInfoDAO.getCatalogId());
    }
    return new CatalogAndSchemaNames(catalogInfoDAO.getName(), schemaInfoDAO.getName());
  }
}
