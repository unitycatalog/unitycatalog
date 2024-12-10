package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateExternalLocation;
import io.unitycatalog.server.model.ExternalLocationInfo;
import io.unitycatalog.server.model.UpdateExternalLocation;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.dao.StorageCredentialDAO;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.*;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;

public class ExternalLocationRepository {
  private static final ExternalLocationRepository INSTANCE = new ExternalLocationRepository();
  private static final StorageCredentialRepository STORAGE_CREDENTIAL_REPOSITORY =
      StorageCredentialRepository.getInstance();
  private static final SessionFactory SESSION_FACTORY = HibernateUtils.getSessionFactory();
  private static final PagedListingHelper<ExternalLocationDAO> LISTING_HELPER =
      new PagedListingHelper<>(ExternalLocationDAO.class);

  private ExternalLocationRepository() {}

  public static ExternalLocationRepository getInstance() {
    return INSTANCE;
  }

  public ExternalLocationDAO addExternalLocation(CreateExternalLocation createExternalLocation) {
    ValidationUtils.validateSqlObjectName(createExternalLocation.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();

    try (Session session = SESSION_FACTORY.openSession()) {
      StorageCredentialDAO storageCredentialDAO =
          STORAGE_CREDENTIAL_REPOSITORY.getStorageCredentialDAO(
              session, createExternalLocation.getCredentialName());
      UUID externalLocationId = UUID.randomUUID();
      ExternalLocationDAO externalLocationDAO =
          ExternalLocationDAO.builder()
              .id(externalLocationId)
              .name(createExternalLocation.getName())
              .url(createExternalLocation.getUrl())
              .readOnly(createExternalLocation.getReadOnly())
              .comment(createExternalLocation.getComment())
              .owner(callerId)
              .accessPoint(createExternalLocation.getAccessPoint())
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
        tx.commit();
        return externalLocationDAO;
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public ExternalLocationInfo getExternalLocation(String name) {
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        ExternalLocationDAO dao = getExternalLocationDAO(session, name);
        if (dao == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "External location not found: " + name);
        }
        tx.commit();
        return dao.toExternalLocationInfo();
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public ExternalLocationDAO getExternalLocationDAO(Session session, String name) {
    Query<ExternalLocationDAO> query =
        session.createQuery(
            "FROM ExternalLocationDAO WHERE name = :value", ExternalLocationDAO.class);
    query.setParameter("value", name);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  public List<ExternalLocationInfo> listExternalLocations(
      Optional<Integer> maxResults, Optional<String> pageToken) {
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        List<ExternalLocationDAO> daoList =
            LISTING_HELPER.listEntity(session, maxResults, pageToken, null);
        List<ExternalLocationInfo> results = new ArrayList<>();
        for (ExternalLocationDAO dao : daoList) {
          results.add(dao.toExternalLocationInfo());
        }
        tx.commit();
        return results;
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public ExternalLocationInfo updateExternalLocation(
      String name, UpdateExternalLocation updateExternalLocation) {
    String callerId = IdentityUtils.findPrincipalEmailAddress();

    try (Session session = SESSION_FACTORY.openSession()) {
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
        if (updateExternalLocation.getReadOnly() != null) {
          existingLocation.setReadOnly(updateExternalLocation.getReadOnly());
        }
        if (updateExternalLocation.getAccessPoint() != null) {
          existingLocation.setAccessPoint(updateExternalLocation.getAccessPoint());
        }
        if (updateExternalLocation.getCredentialName() != null) {
          StorageCredentialDAO storageCredentialDAO =
              STORAGE_CREDENTIAL_REPOSITORY.getStorageCredentialDAO(
                  session, updateExternalLocation.getCredentialName());
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
        tx.commit();
        return existingLocation.toExternalLocationInfo();
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public ExternalLocationDAO deleteExternalLocation(String name) {
    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        ExternalLocationDAO existingLocation = getExternalLocationDAO(session, name);
        if (existingLocation == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "External location not found: " + name);
        }
        session.remove(existingLocation);
        tx.commit();
        return existingLocation;
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }
}
