package io.unitycatalog.server.persist;

import io.unitycatalog.server.persist.dao.DependencyDAO;
import io.unitycatalog.server.persist.dao.DependencyDAO.DependentType;
import java.util.List;
import java.util.UUID;
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
