package io.unitycatalog.server.persist;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.ListCredentialsResponse;
import io.unitycatalog.server.model.UpdateCredentialRequest;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CredentialRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(CredentialRepository.class);
  private final Repositories repositories;
  private final SessionFactory sessionFactory;
  private static final PagedListingHelper<CredentialDAO> LISTING_HELPER =
      new PagedListingHelper<>(CredentialDAO.class);
  public static ObjectMapper objectMapper = new ObjectMapper();

  public CredentialRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
  }

  public CredentialInfo addCredential(CreateCredentialRequest createCredentialRequest) {
    ValidationUtils.validateSqlObjectName(createCredentialRequest.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    UUID storageCredentialId = UUID.randomUUID();
    CredentialInfo storageCredentialInfo =
        new CredentialInfo()
            .id(storageCredentialId.toString())
            .name(createCredentialRequest.getName())
            .comment(createCredentialRequest.getComment())
            .purpose(createCredentialRequest.getPurpose())
            .owner(callerId)
            .createdAt(Instant.now().toEpochMilli())
            .createdBy(callerId);

    if (createCredentialRequest.getAwsIamRole() != null) {
      storageCredentialInfo.setAwsIamRole(
          fromAwsIamRoleRequest(createCredentialRequest.getAwsIamRole()));
    } else {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Storage credential must have one of aws_iam_role, azure_service_principal, azure_managed_identity or gcp_service_account");
    }

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          if (getCredentialDAO(session, createCredentialRequest.getName()) != null) {
            throw new BaseException(
                ErrorCode.ALREADY_EXISTS,
                "Storage credential already exists: " + createCredentialRequest.getName());
          }
          session.persist(CredentialDAO.from(storageCredentialInfo));
          LOGGER.info("Added storage credential: {}", storageCredentialInfo.getName());
          return storageCredentialInfo;
        },
        "Failed to add storage credential",
        /* readOnly = */ false);
  }

  public CredentialInfo getCredential(String name) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          CredentialDAO dao = getCredentialDAO(session, name);
          if (dao == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Storage credential not found: " + name);
          }
          LOGGER.info("Retrieved storage credential: {}", name);
          return dao.toCredentialInfo();
        },
        "Failed to get storage credential",
        /* readOnly = */ true);
  }

  protected CredentialDAO getCredentialDAO(Session session, String name) {
    Query<CredentialDAO> query =
        session.createQuery("FROM CredentialDAO WHERE name = :value", CredentialDAO.class);
    query.setParameter("value", name);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  public ListCredentialsResponse listCredentials(
      Optional<Integer> maxResults, Optional<String> pageToken) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          List<CredentialDAO> daoList =
              LISTING_HELPER.listEntity(
                  session, maxResults, pageToken, /* parentEntityId = */ null);
          String nextPageToken = LISTING_HELPER.getNextPageToken(daoList, maxResults);
          List<CredentialInfo> results = new ArrayList<>();
          for (CredentialDAO dao : daoList) {
            try {
              results.add(dao.toCredentialInfo());
            } catch (Exception e) {
              // Skip credentials that can't be processed
              LOGGER.error("Failed to process credential: {}", dao.getName(), e);
            }
          }
          return new ListCredentialsResponse().credentials(results).nextPageToken(nextPageToken);
        },
        "Failed to list storage credentials",
        /* readOnly = */ true);
  }

  public CredentialInfo updateCredential(String name, UpdateCredentialRequest updateCredential) {
    String callerId = IdentityUtils.findPrincipalEmailAddress();

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          CredentialDAO existingCredential = getCredentialDAO(session, name);
          if (existingCredential == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Storage credential not found: " + name);
          }

          // Update fields if provided
          if (updateCredential.getNewName() != null) {
            ValidationUtils.validateSqlObjectName(updateCredential.getNewName());
            if (getCredentialDAO(session, updateCredential.getNewName()) != null) {
              throw new BaseException(
                  ErrorCode.ALREADY_EXISTS,
                  "Storage credential already exists: " + updateCredential.getNewName());
            }
            existingCredential.setName(updateCredential.getNewName());
          }
          updateCredentialFields(existingCredential, updateCredential);
          if (updateCredential.getComment() != null) {
            existingCredential.setComment(updateCredential.getComment());
          }
          existingCredential.setUpdatedAt(new Date());
          existingCredential.setUpdatedBy(callerId);

          session.merge(existingCredential);
          LOGGER.info("Updated storage credential: {}", name);
          return existingCredential.toCredentialInfo();
        },
        "Failed to update storage credential",
        /* readOnly = */ false);
  }

  private static void updateCredentialFields(
      CredentialDAO existingCredential, UpdateCredentialRequest updateCredentialRequest) {
    try {
      if (updateCredentialRequest.getAwsIamRole() != null) {
        existingCredential.setCredentialType(CredentialDAO.CredentialType.AWS_IAM_ROLE);
        String jsonCredential =
            objectMapper.writeValueAsString(
                fromAwsIamRoleRequest(updateCredentialRequest.getAwsIamRole()));
        // TODO: encrypt the credential
        existingCredential.setCredential(jsonCredential);
      }
    } catch (JsonProcessingException e) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Failed to serialize credential: " + e.getMessage());
    }
  }

  public CredentialInfo deleteCredential(String name) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          CredentialDAO existingCredential = getCredentialDAO(session, name);
          if (existingCredential == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Credential not found: " + name);
          }
          // Convert to CredentialInfo before removing from database
          CredentialInfo credentialInfo = existingCredential.toCredentialInfo();
          session.remove(existingCredential);
          LOGGER.info("Deleted credential: {}", name);
          return credentialInfo;
        },
        "Failed to delete credential",
        /* readOnly = */ false);
  }

  private static AwsIamRoleResponse fromAwsIamRoleRequest(AwsIamRoleRequest awsIamRoleRequest) {
    // TODO: add external id and unity catalog server iam role
    return new AwsIamRoleResponse().roleArn(awsIamRoleRequest.getRoleArn());
  }
}
