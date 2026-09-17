package io.unitycatalog.server.persist;

import io.unitycatalog.server.persist.dao.PropertyDAO;
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

  /** Persists properties, flushing so a column overflow is reported by {@link #flush}. */
  public static void persistAll(Session session, List<PropertyDAO> properties) {
    properties.forEach(session::persist);
    flush(session, PropertyDAO.toMap(properties));
  }

  /**
   * Flushes property writes, logging {@code properties} if the flush fails. Flushing here reports a
   * column overflow while the keys and values are still known: JDBC omits bind parameters, so
   * otherwise the failure gives no hint which property was too long.
   */
  public static void flush(Session session, Map<String, String> properties) {
    try {
      session.flush();
    } catch (RuntimeException e) {
      LOGGER.error("Failed to persist properties:{}", describe(properties), e);
      throw e;
    }
  }

  static String describe(Map<String, String> properties) {
    return properties.entrySet().stream()
        .map(p -> String.format("%n  %s (%d chars) = %.8192s", p.getKey(), length(p), p.getValue()))
        .collect(Collectors.joining());
  }

  private static int length(Map.Entry<String, String> property) {
    return property.getValue() == null ? 0 : property.getValue().length();
  }
}
