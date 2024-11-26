package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateStorageCredential;
import io.unitycatalog.server.model.StorageCredentialInfo;
import io.unitycatalog.server.model.UpdateStorageCredential;
import io.unitycatalog.server.persist.dao.StorageCredentialDAO;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.StorageCredentialUtils;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import java.time.Instant;
import java.util.*;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;

public class StorageCredentialRepository {
  private static final StorageCredentialRepository INSTANCE = new StorageCredentialRepository();
  private static final SessionFactory SESSION_FACTORY = HibernateUtils.getSessionFactory();
  private static final PagedListingHelper<StorageCredentialDAO> LISTING_HELPER =
      new PagedListingHelper<>(StorageCredentialDAO.class);

  private StorageCredentialRepository() {}

  public static StorageCredentialRepository getInstance() {
    return INSTANCE;
  }

  public StorageCredentialInfo addStorageCredential(
      CreateStorageCredential createStorageCredential) {
    ValidationUtils.validateSqlObjectName(createStorageCredential.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    UUID storageCredentialId = UUID.randomUUID();
    StorageCredentialInfo storageCredentialInfo =
        new StorageCredentialInfo()
            .id(storageCredentialId.toString())
            .name(createStorageCredential.getName())
            .comment(createStorageCredential.getComment())
            .readOnly(createStorageCredential.getReadOnly())
            .owner(callerId)
            .createdAt(Instant.now().toEpochMilli())
            .createdBy(callerId)
            .usedForManagedStorage(false);

    if (createStorageCredential.getAwsIamRole() != null) {
      storageCredentialInfo.setAwsIamRole(
          StorageCredentialUtils.fromAwsIamRoleRequest(createStorageCredential.getAwsIamRole()));
    } else if (createStorageCredential.getAzureServicePrincipal() != null) {
      storageCredentialInfo.setAzureServicePrincipal(
          createStorageCredential.getAzureServicePrincipal());
    } else if (createStorageCredential.getAzureManagedIdentity() != null) {
      storageCredentialInfo.setAzureManagedIdentity(
          StorageCredentialUtils.fromAzureManagedIdentityRequest(
              createStorageCredential.getAzureManagedIdentity(), storageCredentialId.toString()));
    } else {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Storage credential must have one of aws_iam_role, azure_service_principal, azure_managed_identity or gcp_service_account");
    }

    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        if (getStorageCredentialDAO(session, createStorageCredential.getName()) != null) {
          throw new BaseException(
              ErrorCode.ALREADY_EXISTS,
              "Storage credential already exists: " + createStorageCredential.getName());
        }
        session.persist(StorageCredentialDAO.from(storageCredentialInfo));
        tx.commit();
        return storageCredentialInfo;
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public StorageCredentialInfo getStorageCredential(String name) {
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        StorageCredentialDAO dao = getStorageCredentialDAO(session, name);
        if (dao == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "Storage credential not found: " + name);
        }
        tx.commit();
        return dao.toStorageCredentialInfo();
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  protected StorageCredentialDAO getStorageCredentialDAO(Session session, String name) {
    Query<StorageCredentialDAO> query =
        session.createQuery(
            "FROM StorageCredentialDAO WHERE name = :value", StorageCredentialDAO.class);
    query.setParameter("value", name);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  public List<StorageCredentialInfo> listStorageCredentials(
      Optional<Integer> maxResults, Optional<String> pageToken) {
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        List<StorageCredentialDAO> daoList =
            LISTING_HELPER.listEntity(session, maxResults, pageToken, null);
        List<StorageCredentialInfo> results = new ArrayList<>();
        for (StorageCredentialDAO dao : daoList) {
          results.add(dao.toStorageCredentialInfo());
        }
        tx.commit();
        return results;
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public StorageCredentialInfo updateStorageCredential(
      String name, UpdateStorageCredential updateStorageCredential) {
    String callerId = IdentityUtils.findPrincipalEmailAddress();

    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        StorageCredentialDAO existingCredential = getStorageCredentialDAO(session, name);
        if (existingCredential == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "Storage credential not found: " + name);
        }

        // Update fields if provided
        if (updateStorageCredential.getNewName() != null) {
          ValidationUtils.validateSqlObjectName(updateStorageCredential.getNewName());
          if (getStorageCredentialDAO(session, updateStorageCredential.getNewName()) != null) {
            throw new BaseException(
                ErrorCode.ALREADY_EXISTS,
                "Storage credential already exists: " + updateStorageCredential.getNewName());
          }
          existingCredential.setName(updateStorageCredential.getNewName());
        }
        updateCredentialFields(existingCredential, updateStorageCredential);
        if (updateStorageCredential.getComment() != null) {
          existingCredential.setComment(updateStorageCredential.getComment());
        }
        if (updateStorageCredential.getReadOnly() != null) {
          existingCredential.setReadOnly(updateStorageCredential.getReadOnly());
        }
        existingCredential.setUpdatedAt(new Date());
        existingCredential.setUpdatedBy(callerId);

        session.merge(existingCredential);
        tx.commit();
        return existingCredential.toStorageCredentialInfo();
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public static void updateCredentialFields(
      StorageCredentialDAO existingCredential, UpdateStorageCredential updateStorageCredential) {
    if (updateStorageCredential.getAwsIamRole() != null) {
      existingCredential.setCredentialType(StorageCredentialDAO.CredentialType.AWS_IAM_ROLE);
      existingCredential.setCredential(
          StorageCredentialUtils.fromAwsIamRoleRequest(updateStorageCredential.getAwsIamRole())
              .toString());
    } else if (updateStorageCredential.getAzureServicePrincipal() != null) {
      existingCredential.setCredentialType(
          StorageCredentialDAO.CredentialType.AZURE_SERVICE_PRINCIPAL);
      existingCredential.setCredential(
          updateStorageCredential.getAzureServicePrincipal().toString());
    } else if (updateStorageCredential.getAzureManagedIdentity() != null) {
      existingCredential.setCredentialType(
          StorageCredentialDAO.CredentialType.AZURE_MANAGED_IDENTITY);
      existingCredential.setCredential(
          StorageCredentialUtils.fromAzureManagedIdentityRequest(
                  updateStorageCredential.getAzureManagedIdentity(),
                  existingCredential.getId().toString())
              .toString());
    } else {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Storage credential must have one of aws_iam_role, azure_service_principal, azure_managed_identity or gcp_service_account");
    }
  }

  public void deleteStorageCredential(String name) {
    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        StorageCredentialDAO existingCredential = getStorageCredentialDAO(session, name);
        if (existingCredential == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "Storage credential not found: " + name);
        }
        session.remove(existingCredential);
        tx.commit();
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }
}
