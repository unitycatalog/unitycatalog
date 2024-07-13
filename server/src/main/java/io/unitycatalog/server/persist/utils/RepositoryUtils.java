package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.persist.PropertyRepository;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.hibernate.Session;

public class RepositoryUtils {
  public static <T> T attachProperties(
      T entityInfo, String uuid, String entityType, Session session) {
    try {
      List<PropertyDAO> propertyDAOList =
          PropertyRepository.findProperties(session, UUID.fromString(uuid), entityType);
      Method setPropertiesMethod = entityInfo.getClass().getMethod("setProperties", Map.class);
      setPropertiesMethod.invoke(entityInfo, PropertyDAO.toMap(propertyDAOList));
      return entityInfo;
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
