package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.*;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.query.Query;

/**
 * Helper class to list entities in a paged manner. Entities are listed in ascending order of their
 * name. The name of the last entity in the list can be used as a page token to fetch the next page.
 *
 * @param <T> The DAO class of the entity to be listed
 */
public class PagedListingHelper<T extends IdentifiableDAO> {

  private final Class<T> entityClass;

  public PagedListingHelper(Class<T> entityClass) {
    this.entityClass = entityClass;
  }

  public static final Integer DEFAULT_PAGE_SIZE = 100;

  /**
   * Get the page size to use for listing entities. The page size is the minimum of the maxResults
   * and the default page size.
   *
   * @param maxResults The maximum number of results to return.
   * @return The page size to use for listing entities.
   */
  public static Integer getPageSize(Optional<Integer> maxResults) {
    return maxResults
        .filter(x -> (x > 0))
        .map(x -> Math.min(x, DEFAULT_PAGE_SIZE))
        .orElse(DEFAULT_PAGE_SIZE);
  }

  /**
   * This function returns the next page token to use to fetch the next page of entities. The next
   * page token is the name of the last entity in the list.
   *
   * @param entities The list of entities to check
   * @param maxResults The maximum number of results to return
   * @return The next page token to use to fetch the next page of entities. Returns null if there
   *     are no more entities to fetch.
   */
  public String getNextPageToken(List<T> entities, Optional<Integer> maxResults) {
    if (entities == null || entities.isEmpty() || entities.size() < getPageSize(maxResults)) {
      return null;
    }
    // return the last entity name
    return entities.get(entities.size() - 1).getName();
  }

  /**
   * This function builds a query to fetch the next page of entities. The query fetches entities
   * whose name is greater than the page token.
   *
   * @param session The Hibernate session
   * @param parentEntityId The parent entity id
   * @param pageToken The page token
   * @return The query to fetch the next page of entities
   */
  public Query<T> buildListQuery(Session session, UUID parentEntityId, String pageToken) {
    CriteriaBuilder cb = session.getCriteriaBuilder();
    CriteriaQuery<T> cr = cb.createQuery(entityClass);
    Root<T> root = cr.from(entityClass);

    List<Predicate> predicates = new ArrayList<>();
    Optional<String> parentEntityIdColumn = IdentifiableDAO.getParentIdColumnName(entityClass);
    parentEntityIdColumn.ifPresent(s -> predicates.add(cb.equal(root.get(s), parentEntityId)));
    predicates.add(
        cb.or(cb.greaterThan(root.get("name"), pageToken), cb.isNull(cb.literal(pageToken))));

    Predicate combinedPredicate = cb.and(predicates.toArray(new Predicate[0]));
    cr.select(root).where(combinedPredicate).orderBy(cb.asc(root.get("name")));

    return session.createQuery(cr);
  }

  /**
   * This function lists entities in a paged manner. The entities are listed in ascending order of
   * their name. The name of the last entity in the list can be used as a page token to fetch the
   * next page.
   *
   * @param session The Hibernate session
   * @param maxResultsOpt The maximum number of results to return
   * @param nextPageTokenOpt The page token to use to fetch the next page of entities
   * @param parentEntityId The parent entity id
   * @return the list of entities based on the input parameters
   */
  public List<T> listEntity(
      Session session,
      Optional<Integer> maxResultsOpt,
      Optional<String> nextPageTokenOpt,
      UUID parentEntityId) {
    if (maxResultsOpt.isPresent() && maxResultsOpt.get() < 0) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "maxResults must be greater than or equal to 0");
    }
    Integer pageSize = getPageSize(maxResultsOpt);
    Query<T> query = buildListQuery(session, parentEntityId, nextPageTokenOpt.orElse(null));
    query.setMaxResults(pageSize);
    return query.getResultList();
  }
}
