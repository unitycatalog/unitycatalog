package io.unitycatalog.server.persist;

import io.unitycatalog.server.persist.dao.DependencyDAO;
import io.unitycatalog.server.persist.dao.DependencyDAO.DependentType;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repository for managing view/metric-view dependency records in uc_dependencies. Methods accept a
 * Session so they can participate in the caller's transaction.
 */
public class DependencyRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(DependencyRepository.class);

  public void createDependencies(
      Session session,
      UUID dependentId,
      DependentType dependentType,
      List<DependencyDAO> dependencies) {
    for (DependencyDAO dep : dependencies) {
      dep.setDependentId(dependentId);
      dep.setDependentType(dependentType);
      session.persist(dep);
    }
    LOGGER.debug(
        "Created {} dependencies for {}:{}", dependencies.size(), dependentType, dependentId);
  }

  public List<DependencyDAO> getDependencies(
      Session session, UUID dependentId, DependentType dependentType) {
    String hql =
        "FROM DependencyDAO d WHERE d.dependentId = :dependentId"
            + " AND d.dependentType = :dependentType";
    Query<DependencyDAO> query = session.createQuery(hql, DependencyDAO.class);
    query.setParameter("dependentId", dependentId);
    query.setParameter("dependentType", dependentType);
    return query.list();
  }

  /**
   * Bulk variant of {@link #getDependencies} for listing endpoints: loads the dependencies of all
   * {@code dependentIds} in one query and groups them by dependent ID. Dependents without
   * dependencies have no entry in the returned map.
   */
  public Map<UUID, List<DependencyDAO>> getDependenciesByDependentIds(
      Session session, Collection<UUID> dependentIds, DependentType dependentType) {
    if (dependentIds.isEmpty()) {
      return Map.of();
    }
    String hql =
        "FROM DependencyDAO d WHERE d.dependentId IN (:dependentIds)"
            + " AND d.dependentType = :dependentType";
    Query<DependencyDAO> query = session.createQuery(hql, DependencyDAO.class);
    query.setParameter("dependentIds", dependentIds);
    query.setParameter("dependentType", dependentType);
    return query.list().stream().collect(Collectors.groupingBy(DependencyDAO::getDependentId));
  }

  public void deleteDependencies(Session session, UUID dependentId, DependentType dependentType) {
    String hql =
        "DELETE FROM DependencyDAO d WHERE d.dependentId = :dependentId"
            + " AND d.dependentType = :dependentType";
    Query<?> query = session.createQuery(hql);
    query.setParameter("dependentId", dependentId);
    query.setParameter("dependentType", dependentType);
    int deleted = query.executeUpdate();
    LOGGER.debug("Deleted {} dependencies for {}:{}", deleted, dependentType, dependentId);
  }
}
