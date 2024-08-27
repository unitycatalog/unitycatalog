package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.PropertyRepository;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.utils.Constants;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.HashMap;

import org.hibernate.Session;

public class RepositoryUtils {
  private static final SchemaRepository SCHEMA_REPOSITORY = SchemaRepository.getInstance();
  private static final CatalogRepository CATALOG_REPOSITORY = CatalogRepository.getInstance();

  private static final Map<String, Class<?>> PROPERTY_TYPE_MAP = new HashMap<>();
  static {
    PROPERTY_TYPE_MAP.put(Constants.FUNCTION, String.class);
  }
  public static <T> T attachProperties(
      T entityInfo, String uuid, String entityType, Session session) {
    try {
      List<PropertyDAO> propertyDAOList =
          PropertyRepository.findProperties(session, UUID.fromString(uuid), entityType);
      Class<?> entityClass = PROPERTY_TYPE_MAP.getOrDefault(entityType, Map.class);
      Method setPropertiesMethod =
          entityInfo.getClass().getMethod("setProperties", entityClass);
      Map<String, String> propertyMap = PropertyDAO.toMap(propertyDAOList);
      Object propertiesArgument = switch (entityClass.getSimpleName()) {
          case "Map" -> propertyMap;
          case "String" -> propertyMap.toString();
          default -> throw new IllegalArgumentException("Unsupported parameter type: " + entityClass.getSimpleName());
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

  public static UUID getSchemaId(Session session, String catalogName, String schemaName) {
    SchemaInfoDAO schemaInfo = SCHEMA_REPOSITORY.getSchemaDAO(session, catalogName, schemaName);
    if (schemaInfo == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + schemaName);
    }
    return schemaInfo.getId();
  }

  public static UUID getCatalogId(Session session, String catalogName) {
    CatalogInfoDAO catalogInfo = CATALOG_REPOSITORY.getCatalogDAO(session, catalogName);
    if (catalogInfo == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + catalogName);
    }
    return catalogInfo.getId();
  }
}
