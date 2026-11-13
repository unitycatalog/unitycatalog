package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AzureServicePrincipalRequest;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.GcpServiceAccountKeyRequest;
import io.unitycatalog.server.model.ListCredentialsResponse;
import io.unitycatalog.server.model.UpdateCredentialRequest;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.CredentialDAO.CredentialType;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ValidationUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CredentialRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(CredentialRepository.class);
  private final Repositories repositories;
  private final SessionFactory sessionFactory;
  private final ServerProperties serverProperties;
  private static final PagedListingHelper<CredentialDAO> LISTING_HELPER =
      new PagedListingHelper<>(CredentialDAO.class);

  public CredentialRepository(
      Repositories repositories, SessionFactory sessionFactory, ServerProperties serverProperties) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
    this.serverProperties = serverProperties;
  }

  public CredentialInfo addCredential(CreateCredentialRequest createCredentialRequest) {
    ValidationUtils.validateSqlObjectName(createCredentialRequest.getName());
    validateCloudIdentity(createCredentialRequest);
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    CredentialDAO dao = CredentialDAO.from(createCredentialRequest, callerId);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          if (getCredentialDAO(session, createCredentialRequest.getName()) != null) {
            throw new BaseException(
                ErrorCode.STORAGE_CREDENTIAL_ALREADY_EXISTS,
                "Storage credential already exists: " + createCredentialRequest.getName());
          }
          session.persist(dao);
          LOGGER.info("Added storage credential: {}", dao.getName());
          return dao.toCredentialInfo(getAwsS3MasterRoleArn());
        },
        "Failed to add storage credential",
        /* readOnly= */ false);
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
          return dao.toCredentialInfo(getAwsS3MasterRoleArn());
        },
        "Failed to get storage credential",
        /* readOnly= */ true);
  }

  /** Returns null if the credential no longer exists, e.g. after a force delete. */
  protected String getCredentialName(Session session, UUID id) {
    CredentialDAO credentialDAO = session.get(CredentialDAO.class, id);
    return credentialDAO == null ? null : credentialDAO.getName();
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
              LISTING_HELPER.listEntity(session, maxResults, pageToken, /* parentEntityId= */ null);
          String nextPageToken = LISTING_HELPER.getNextPageToken(daoList, maxResults);
          List<CredentialInfo> results = new ArrayList<>();
          for (CredentialDAO dao : daoList) {
            try {
              results.add(dao.toCredentialInfo(getAwsS3MasterRoleArn()));
            } catch (Exception e) {
              // Skip credentials that can't be processed
              LOGGER.error("Failed to process credential: {}", dao.getName(), e);
            }
          }
          return new ListCredentialsResponse().credentials(results).nextPageToken(nextPageToken);
        },
        "Failed to list storage credentials",
        /* readOnly= */ true);
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
                  ErrorCode.STORAGE_CREDENTIAL_ALREADY_EXISTS,
                  "Storage credential already exists: " + updateCredential.getNewName());
            }
            existingCredential.setName(updateCredential.getNewName());
          }
          if (updateCredential.getAwsIamRole() != null) {
            // setAwsIamRole also changes the type, which would turn an Azure or GCS credential
            // into an AWS one under the external locations that use it.
            ValidationUtils.checkArgument(
                existingCredential.getCredentialType() == CredentialType.AWS_IAM_ROLE,
                "Storage credential '%s' is not an AWS IAM role credential, so aws_iam_role cannot"
                    + " be set on it.",
                name);
            existingCredential.setAwsIamRole(updateCredential.getAwsIamRole());
          }
          if (updateCredential.getComment() != null) {
            existingCredential.setComment(updateCredential.getComment());
          }
          existingCredential.setUpdatedAt(new Date());
          existingCredential.setUpdatedBy(callerId);

          session.merge(existingCredential);
          LOGGER.info("Updated storage credential: {}", name);
          return existingCredential.toCredentialInfo(getAwsS3MasterRoleArn());
        },
        "Failed to update storage credential",
        /* readOnly= */ false);
  }

  public UUID deleteCredential(String name, boolean force) {
    LOGGER.debug("Deleting storage credential {}", name);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          CredentialDAO existingCredential = getCredentialDAO(session, name);
          if (existingCredential == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Credential not found: " + name);
          }
          if (!force) {
            // Check if it's still used by any external location.
            ExternalLocationDAO externalLocationDAO =
                getExternalLocationDAOUsingCredential(session, existingCredential.getId());
            if (externalLocationDAO != null) {
              throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT,
                  "Credential still used by external location '"
                      + externalLocationDAO.getName()
                      + "'");
            }
          }
          session.remove(existingCredential);
          LOGGER.info("Deleted credential: {}", name);
          return existingCredential.getId();
        },
        "Failed to delete credential",
        /* readOnly= */ false);
  }

  protected ExternalLocationDAO getExternalLocationDAOUsingCredential(
      Session session, UUID credentialId) {
    Query<ExternalLocationDAO> query =
        session.createQuery(
            "FROM ExternalLocationDAO WHERE credentialId = :value", ExternalLocationDAO.class);
    query.setParameter("value", credentialId);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  /**
   * Checks that a create request carries exactly one cloud identity and that every field of it is
   * set. The generated models do not enforce required fields, and nothing calls the cloud at create
   * time, so without this an incomplete identity would only fail when a credential is first vended.
   */
  private static void validateCloudIdentity(CreateCredentialRequest request) {
    long identities =
        Stream.of(
                request.getAwsIamRole(),
                request.getAzureServicePrincipal(),
                request.getGcpServiceAccountKey())
            .filter(Objects::nonNull)
            .count();
    ValidationUtils.checkArgument(
        identities == 1,
        "Exactly one of aws_iam_role, azure_service_principal or gcp_service_account_key must be"
            + " set.");
    if (request.getAwsIamRole() != null) {
      checkNotBlank(request.getAwsIamRole().getRoleArn(), "aws_iam_role.role_arn");
    }
    AzureServicePrincipalRequest servicePrincipal = request.getAzureServicePrincipal();
    if (servicePrincipal != null) {
      checkNotBlank(servicePrincipal.getDirectoryId(), "azure_service_principal.directory_id");
      checkNotBlank(servicePrincipal.getApplicationId(), "azure_service_principal.application_id");
      checkNotBlank(servicePrincipal.getClientSecret(), "azure_service_principal.client_secret");
    }
    GcpServiceAccountKeyRequest key = request.getGcpServiceAccountKey();
    if (key != null) {
      checkNotBlank(key.getEmail(), "gcp_service_account_key.email");
      checkNotBlank(key.getPrivateKeyId(), "gcp_service_account_key.private_key_id");
      checkNotBlank(key.getPrivateKey(), "gcp_service_account_key.private_key");
      checkPrivateKeyLoads(key);
    }
  }

  private static void checkNotBlank(String value, String field) {
    ValidationUtils.checkArgument(
        value != null && !value.isBlank(), "%s must not be empty.", field);
  }

  /** Loads the key as the GCS vendor does when it vends with it. */
  private static void checkPrivateKeyLoads(GcpServiceAccountKeyRequest key) {
    try {
      GcpCredentialVendor.credentialsOf(key);
    } catch (IOException e) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "gcp_service_account_key.private_key is not a PEM-encoded PKCS#8 private key.");
    }
  }

  private Optional<String> getAwsS3MasterRoleArn() {
    return Optional.ofNullable(serverProperties.get(ServerProperties.Property.AWS_MASTER_ROLE_ARN));
  }
}
