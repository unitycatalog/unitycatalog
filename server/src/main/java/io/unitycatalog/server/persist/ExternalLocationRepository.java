package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateExternalLocation;
import io.unitycatalog.server.model.ExternalLocationInfo;
import io.unitycatalog.server.model.ListExternalLocationsResponse;
import io.unitycatalog.server.model.UpdateExternalLocation;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.dao.StorageCredentialDAO;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.*;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalLocationRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalLocationRepository.class);
  private final Repositories repositories;
  private final SessionFactory sessionFactory;
  private static final PagedListingHelper<ExternalLocationDAO> LISTING_HELPER =
      new PagedListingHelper<>(ExternalLocationDAO.class);

  public ExternalLocationRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
  }

  public ExternalLocationDAO addExternalLocation(CreateExternalLocation createExternalLocation) {
    ValidationUtils.validateSqlObjectName(createExternalLocation.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();

    try (Session session = sessionFactory.openSession()) {
      StorageCredentialDAO storageCredentialDAO =
          repositories
              .getStorageCredentialRepository()
              .getStorageCredentialDAO(session, createExternalLocation.getCredentialName());
      if (storageCredentialDAO == null) {
        throw new BaseException(
            ErrorCode.NOT_FOUND,
            "Storage credential not found: " + createExternalLocation.getCredentialName());
      }
      UUID externalLocationId = UUID.randomUUID();
      ExternalLocationDAO externalLocationDAO =
          ExternalLocationDAO.builder()
              .id(externalLocationId)
              .name(createExternalLocation.getName())
              .url(createExternalLocation.getUrl())
              .comment(createExternalLocation.getComment())
              .owner(callerId)
              .credentialId(storageCredentialDAO.getId())
              .createdAt(new Date())
              .createdBy(callerId)
              .build();
      Transaction tx = session.beginTransaction();
      try {
        if (getExternalLocationDAO(session, createExternalLocation.getName()) != null) {
          throw new BaseException(
              ErrorCode.ALREADY_EXISTS,
              "External location already exists: " + createExternalLocation.getName());
        }
        session.persist(externalLocationDAO);
        LOGGER.info("External location added: {}", externalLocationDAO.getName());
        tx.commit();
        return externalLocationDAO;
      } catch (Exception e) {
        tx.rollback();
        LOGGER.error("Failed to add external location", e);
        throw e;
      }
    }
  }

  public ExternalLocationInfo getExternalLocation(String name) {
    try (Session session = sessionFactory.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        ExternalLocationDAO externalLocationDAO = getExternalLocationDAO(session, name);
        if (externalLocationDAO == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "External location not found: " + name);
        }
        LOGGER.info("External location retrieved: {}", externalLocationDAO.getName());
        tx.commit();
        return externalLocationDAO.toExternalLocationInfo();
      } catch (Exception e) {
        tx.rollback();
        LOGGER.error("Failed to get external location", e);
        throw e;
      }
    }
  }

  private ExternalLocationDAO getExternalLocationDAO(Session session, String name) {
    Query<ExternalLocationDAO> query =
        session.createQuery(
            "FROM ExternalLocationDAO WHERE name = :value", ExternalLocationDAO.class);
    query.setParameter("value", name);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  public ListExternalLocationsResponse listExternalLocations(
      Optional<Integer> maxResults, Optional<String> pageToken) {
    try (Session session = sessionFactory.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        List<ExternalLocationDAO> daoList =
            LISTING_HELPER.listEntity(session, maxResults, pageToken, /* parentEntityId = */ null);
        String nextPageToken = LISTING_HELPER.getNextPageToken(daoList, maxResults);
        List<ExternalLocationInfo> results = new ArrayList<>();
        for (ExternalLocationDAO dao : daoList) {
          results.add(dao.toExternalLocationInfo());
        }
        tx.commit();
        return new ListExternalLocationsResponse()
            .externalLocations(results)
            .nextPageToken(nextPageToken);
      } catch (Exception e) {
        tx.rollback();
        LOGGER.error("Failed to list external locations", e);
        throw e;
      }
    }
  }

  public ExternalLocationInfo updateExternalLocation(
      String name, UpdateExternalLocation updateExternalLocation) {
    String callerId = IdentityUtils.findPrincipalEmailAddress();

    try (Session session = sessionFactory.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        ExternalLocationDAO existingLocation = getExternalLocationDAO(session, name);
        if (existingLocation == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "External location not found: " + name);
        }

        // Update fields if provided
        if (updateExternalLocation.getNewName() != null) {
          ValidationUtils.validateSqlObjectName(updateExternalLocation.getNewName());
          if (getExternalLocationDAO(session, updateExternalLocation.getNewName()) != null) {
            throw new BaseException(
                ErrorCode.ALREADY_EXISTS,
                "External location already exists: " + updateExternalLocation.getNewName());
          }
          existingLocation.setName(updateExternalLocation.getNewName());
        }
        if (updateExternalLocation.getUrl() != null) {
          existingLocation.setUrl(updateExternalLocation.getUrl());
        }
        if (updateExternalLocation.getComment() != null) {
          existingLocation.setComment(updateExternalLocation.getComment());
        }
        if (updateExternalLocation.getCredentialName() != null) {
          StorageCredentialDAO storageCredentialDAO =
              repositories
                  .getStorageCredentialRepository()
                  .getStorageCredentialDAO(session, updateExternalLocation.getCredentialName());
          if (storageCredentialDAO == null) {
            throw new BaseException(
                ErrorCode.NOT_FOUND,
                "Storage credential not found: " + updateExternalLocation.getCredentialName());
          }
          existingLocation.setCredentialId(storageCredentialDAO.getId());
        }

        existingLocation.setUpdatedAt(new Date());
        existingLocation.setUpdatedBy(callerId);

        session.merge(existingLocation);
        LOGGER.info("Updated external location: {}", name);
        tx.commit();
        return existingLocation.toExternalLocationInfo();
      } catch (Exception e) {
        tx.rollback();
        LOGGER.error("Failed to update external location", e);
        throw e;
      }
    }
  }

  public ExternalLocationDAO deleteExternalLocation(String name) {
    try (Session session = sessionFactory.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        ExternalLocationDAO existingLocation = getExternalLocationDAO(session, name);
        if (existingLocation == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "External location not found: " + name);
        }
        session.remove(existingLocation);
        LOGGER.info("Deleted external location: {}", name);
        tx.commit();
        return existingLocation;
      } catch (Exception e) {
        tx.rollback();
        LOGGER.error("Failed to delete external location", e);
        throw e;
      }
    }
  }
}
