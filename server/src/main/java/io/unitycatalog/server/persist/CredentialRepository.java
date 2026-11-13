package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsIamRoleRequest;
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
import io.unitycatalog.server.service.credential.gcp.ServiceAccountKeys;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ValidationUtils;
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
    List<CredentialType> cloudIdentities =
        validateCloudIdentities(
            createCredentialRequest.getAwsIamRole(),
            createCredentialRequest.getAzureServicePrincipal(),
            createCredentialRequest.getGcpServiceAccountKey());
    ValidationUtils.checkArgument(
        cloudIdentities.size() == 1, "Exactly one of %s must be set.", CLOUD_IDENTITY_FIELDS);
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
    List<CredentialType> cloudIdentities =
        validateCloudIdentities(
            updateCredential.getAwsIamRole(),
            updateCredential.getAzureServicePrincipal(),
            updateCredential.getGcpServiceAccountKey());
    ValidationUtils.checkArgument(
        cloudIdentities.size() <= 1, "At most one of %s may be set.", CLOUD_IDENTITY_FIELDS);

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
          if (!cloudIdentities.isEmpty()) {
            // External locations bind a credential to URLs of its cloud, so the cloud is fixed.
            CredentialType existingType = existingCredential.getCredentialType();
            ValidationUtils.checkArgument(
                cloudIdentities.get(0) == existingType,
                "Storage credential '%s' holds %s and cannot be changed to %s.",
                name,
                existingType.getApiField(),
                cloudIdentities.get(0).getApiField());
            switch (existingType) {
              case AWS_IAM_ROLE ->
                  existingCredential.setAwsIamRole(updateCredential.getAwsIamRole());
              case AZURE_SERVICE_PRINCIPAL ->
                  existingCredential.setAzureServicePrincipal(
                      updateCredential.getAzureServicePrincipal());
              case GCP_SERVICE_ACCOUNT_KEY ->
                  existingCredential.setGcpServiceAccountKey(
                      updateCredential.getGcpServiceAccountKey());
            }
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

  private Optional<String> getAwsS3MasterRoleArn() {
    return Optional.ofNullable(serverProperties.get(ServerProperties.Property.AWS_MASTER_ROLE_ARN));
  }

  private static final String CLOUD_IDENTITY_FIELDS =
      "aws_iam_role, azure_service_principal or gcp_service_account_key";

  /**
   * The cloud identities a request carries, each checked for the fields the vendor needs. The
   * generated models do not enforce required fields, so a missing field would otherwise surface
   * only when a credential is vended.
   */
  private static List<CredentialType> validateCloudIdentities(
      AwsIamRoleRequest awsIamRole,
      AzureServicePrincipalRequest azureServicePrincipal,
      GcpServiceAccountKeyRequest gcpServiceAccountKey) {
    List<CredentialType> types = new ArrayList<>();
    if (awsIamRole != null) {
      checkNotBlank(awsIamRole.getRoleArn(), "aws_iam_role.role_arn");
      types.add(CredentialType.AWS_IAM_ROLE);
    }
    if (azureServicePrincipal != null) {
      checkNotBlank(azureServicePrincipal.getDirectoryId(), "azure_service_principal.directory_id");
      checkNotBlank(
          azureServicePrincipal.getApplicationId(), "azure_service_principal.application_id");
      checkNotBlank(
          azureServicePrincipal.getClientSecret(), "azure_service_principal.client_secret");
      types.add(CredentialType.AZURE_SERVICE_PRINCIPAL);
    }
    if (gcpServiceAccountKey != null) {
      checkNotBlank(gcpServiceAccountKey.getEmail(), "gcp_service_account_key.email");
      checkNotBlank(
          gcpServiceAccountKey.getPrivateKeyId(), "gcp_service_account_key.private_key_id");
      checkNotBlank(gcpServiceAccountKey.getPrivateKey(), "gcp_service_account_key.private_key");
      ServiceAccountKeys.parse(gcpServiceAccountKey);
      types.add(CredentialType.GCP_SERVICE_ACCOUNT_KEY);
    }
    return types;
  }

  private static void checkNotBlank(String value, String field) {
    ValidationUtils.checkArgument(
        value != null && !value.isBlank(), "%s must not be empty.", field);
  }
}
