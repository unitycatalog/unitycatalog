package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.*;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import org.hibernate.Session;
import org.hibernate.query.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class PagedEntityLister<T extends IdentifiableDAO> {

    private final Class<T> entityClass;

    public PagedEntityLister(Class<T> entityClass) {
        this.entityClass = entityClass;
    }

    public static final Integer DEFAULT_PAGE_SIZE = 100;

    public static Integer getPageSize(Optional<Integer> maxResults) {
        return maxResults
                .filter(x -> (x > 0))
                .map(x -> Math.min(x, DEFAULT_PAGE_SIZE))
                .orElse(DEFAULT_PAGE_SIZE);
    }

    public String getNextPageToken(List<T> entities, Optional<Integer> maxResults) {
        if (entities == null || entities.isEmpty() || entities.size() < getPageSize(maxResults)) {
            return null;
        }
        // return the last entity name
        return entities.get(entities.size() - 1).getName();
    }

    public String getParentEntity() {
        if (entityClass == TableInfoDAO.class) {
            return "schemaId";
        } else if (entityClass == SchemaInfoDAO.class) {
            return "catalogId";
        } else if (entityClass == CatalogInfoDAO.class) {
            return null;
        } else {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid entity type");
        }
    }

    public Query<T> buildListQuery(Session session, UUID parentEntityId, String pageToken) {
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<T> cr = cb.createQuery(entityClass);
        Root<T> root = cr.from(entityClass);

        List<Predicate> predicates = new ArrayList<>();
        String parentEntity = getParentEntity();
        if (parentEntity != null) {
            predicates.add(cb.equal(root.get(parentEntity), parentEntityId));
        }
        predicates.add(cb.or(
                cb.greaterThan(root.get("name"), pageToken),
                cb.isNull(cb.literal(pageToken))
        ));

        Predicate combinedPredicate = cb.and(predicates.toArray(new Predicate[0]));
        cr.select(root).where(combinedPredicate).orderBy(cb.asc(root.get("name")));

        return session.createQuery(cr);
    }

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
