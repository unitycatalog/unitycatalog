package io.unitycatalog.server.persist;

import io.unitycatalog.server.persist.dao.PropertyDAO;
import java.util.List;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(PropertyRepository.class);

  public static List<PropertyDAO> findProperties(
      Session session, UUID entityId, String entityType) {
    LOGGER.debug("Getting properties for {}: {}", entityType, entityId);
    String hql = "FROM PropertyDAO p WHERE p.entityId = :entityId and p.entityType = :entityType";
    Query<PropertyDAO> query = session.createQuery(hql, PropertyDAO.class);
    query.setParameter("entityId", entityId);
    query.setParameter("entityType", entityType);
    return query.list();
  }
}
