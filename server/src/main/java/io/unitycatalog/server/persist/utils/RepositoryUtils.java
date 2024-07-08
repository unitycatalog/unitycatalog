package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.persist.PropertyRepository;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.utils.Constants;
import org.hibernate.Session;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RepositoryUtils {
    public static <T> T attachProperties(T entityInfo, Session session) {
        try {
            Method getIdMethod = entityInfo.getClass().getMethod("getId");
            String id = (String) getIdMethod.invoke(entityInfo);
            List<PropertyDAO> propertyDAOList = PropertyRepository.findProperties(
                    session, UUID.fromString(id), Constants.CATALOG);
            Method setPropertiesMethod = entityInfo.getClass().getMethod("setProperties", Map.class);
            setPropertiesMethod.invoke(entityInfo, PropertyDAO.toMap(propertyDAOList));
            return entityInfo;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Unable to set properties in entity: " + entityInfo.getClass(), e);
        }
    }
}
