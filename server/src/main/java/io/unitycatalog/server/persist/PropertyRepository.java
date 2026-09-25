package io.unitycatalog.server.persist;

import io.unitycatalog.server.persist.dao.PropertyDAO;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
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

  /**
   * Bulk variant of {@link #findProperties} for listing endpoints: loads the properties of all
   * {@code entityIds} in one query and groups them by entity ID. Entities without properties have
   * no entry in the returned map.
   */
  public static Map<UUID, List<PropertyDAO>> findPropertiesByEntityIds(
      Session session, Collection<UUID> entityIds, String entityType) {
    if (entityIds.isEmpty()) {
      return Map.of();
    }
    LOGGER.debug("Getting properties for {} {} entities", entityIds.size(), entityType);
    String hql =
        "FROM PropertyDAO p WHERE p.entityId IN (:entityIds) and p.entityType = :entityType";
    Query<PropertyDAO> query = session.createQuery(hql, PropertyDAO.class);
    query.setParameter("entityIds", entityIds);
    query.setParameter("entityType", entityType);
    return query.list().stream().collect(Collectors.groupingBy(PropertyDAO::getEntityId));
  }
}
