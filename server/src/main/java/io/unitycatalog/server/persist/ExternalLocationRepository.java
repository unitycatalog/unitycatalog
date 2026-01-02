package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateExternalLocation;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.model.ExternalLocationInfo;
import io.unitycatalog.server.model.ListExternalLocationsResponse;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.UpdateExternalLocation;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.IdentityUtils;
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

  /**
   * Validates and retrieves credential DAO for the given credential name.
   *
   * @param session The Hibernate session
   * @param credentialName The name of the credential to validate
   * @return The validated CredentialDAO
   * @throws BaseException if credential doesn't exist or is not of type storage
   */
  private CredentialDAO validateAndGetCredentialDAO(Session session, String credentialName) {
    CredentialDAO credentialDAO =
        repositories.getCredentialRepository().getCredentialDAO(session, credentialName);
    if (credentialDAO == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Credential not found: " + credentialName);
    }
    if (!credentialDAO.getPurpose().equals(CredentialPurpose.STORAGE)) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Credential not of type storage: " + credentialName);
    }
    return credentialDAO;
  }

  public ExternalLocationDAO addExternalLocation(CreateExternalLocation createExternalLocation) {
    ValidationUtils.validateSqlObjectName(createExternalLocation.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          if (getExternalLocationDAO(session, createExternalLocation.getName()) != null) {
            throw new BaseException(
                ErrorCode.ALREADY_EXISTS,
                "External location already exists: " + createExternalLocation.getName());
          }

          String url = FileOperations.toStandardizedURIString(createExternalLocation.getUrl());
          validateUrlNotUsedByAnyExternalLocation(session, url, Optional.empty());

          CredentialDAO credentialDAO =
              validateAndGetCredentialDAO(session, createExternalLocation.getCredentialName());
          UUID externalLocationId = UUID.randomUUID();
          ExternalLocationDAO externalLocationDAO =
              ExternalLocationDAO.builder()
                  .id(externalLocationId)
                  .name(createExternalLocation.getName())
                  .url(url)
                  .comment(createExternalLocation.getComment())
                  .owner(callerId)
                  .credentialId(credentialDAO.getId())
                  .createdAt(new Date())
                  .createdBy(callerId)
                  .build();
          session.persist(externalLocationDAO);
          LOGGER.info("External location added: {}", externalLocationDAO.getName());
          return externalLocationDAO;
        },
        "Failed to add external location",
        /* readOnly = */ false);
  }

  public ExternalLocationInfo getExternalLocation(String name) {
    return getExternalLocationDAO(name).toExternalLocationInfo();
  }

  public ExternalLocationDAO getExternalLocationDAO(String name) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          ExternalLocationDAO externalLocationDAO = getExternalLocationDAO(session, name);
          if (externalLocationDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "External location not found: " + name);
          }
          LOGGER.debug("External location retrieved: {}", externalLocationDAO.getName());
          return externalLocationDAO;
        },
        "Failed to get external location",
        /* readOnly = */ true);
  }

  protected ExternalLocationDAO getExternalLocationDAO(Session session, String name) {
    Query<ExternalLocationDAO> query =
        session.createQuery(
            "FROM ExternalLocationDAO WHERE name = :value", ExternalLocationDAO.class);
    query.setParameter("value", name);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  public ListExternalLocationsResponse listExternalLocations(
      Optional<Integer> maxResults, Optional<String> pageToken) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          List<ExternalLocationDAO> daoList =
              LISTING_HELPER.listEntity(
                  session, maxResults, pageToken, /* parentEntityId = */ null);
          String nextPageToken = LISTING_HELPER.getNextPageToken(daoList, maxResults);
          List<ExternalLocationInfo> results = new ArrayList<>();
          for (ExternalLocationDAO dao : daoList) {
            results.add(dao.toExternalLocationInfo());
          }
          return new ListExternalLocationsResponse()
              .externalLocations(results)
              .nextPageToken(nextPageToken);
        },
        "Failed to list external locations",
        /* readOnly = */ true);
  }

  public ExternalLocationInfo updateExternalLocation(
      String name, UpdateExternalLocation updateExternalLocation) {
    String callerId = IdentityUtils.findPrincipalEmailAddress();

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
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
            existingLocation.setUrl(
                FileOperations.toStandardizedURIString(updateExternalLocation.getUrl()));
            validateUrlNotUsedByAnyExternalLocation(
                session, existingLocation.getUrl(), Optional.of(existingLocation.getId()));
          }
          if (updateExternalLocation.getComment() != null) {
            existingLocation.setComment(updateExternalLocation.getComment());
          }
          if (updateExternalLocation.getCredentialName() != null) {
            CredentialDAO credentialDAO =
                validateAndGetCredentialDAO(session, updateExternalLocation.getCredentialName());
            existingLocation.setCredentialId(credentialDAO.getId());
          }

          existingLocation.setUpdatedAt(new Date());
          existingLocation.setUpdatedBy(callerId);

          session.merge(existingLocation);
          LOGGER.info("Updated external location: {}", name);
          return existingLocation.toExternalLocationInfo();
        },
        "Failed to update external location",
        /* readOnly = */ false);
  }

  public ExternalLocationDAO deleteExternalLocation(String name) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          ExternalLocationDAO existingLocation = getExternalLocationDAO(session, name);
          if (existingLocation == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "External location not found: " + name);
          }
          session.remove(existingLocation);
          LOGGER.info("Deleted external location: {}", name);
          return existingLocation;
        },
        "Failed to delete external location",
        /* readOnly = */ false);
  }

  /**
   * Validates that no other external location has a URL that overlaps with the given URL.
   *
   * <p>This method ensures that external locations have non-overlapping URL hierarchies. It checks
   * for three types of overlap:
   *
   * <ol>
   *   <li>Exact match: An existing location has the same URL
   *   <li>Parent match: An existing location's URL is a parent directory of the given URL
   *   <li>Subdirectory match: An existing location's URL is a subdirectory of the given URL
   * </ol>
   *
   * <p>If any overlap is found, this method throws a BaseException with INVALID_ARGUMENT error
   * code.
   *
   * @param session The Hibernate session for database access
   * @param url The URL to validate (must be standardized)
   * @param currentExternalLocationId The ID of the current external location it's validating. This
   *     is only set for updating an existing external location. For creating new ones it's empty.
   * @throws BaseException if an overlapping external location exists
   */
  private void validateUrlNotUsedByAnyExternalLocation(
      Session session, String url, Optional<UUID> currentExternalLocationId) {
    ExternalLocationUtils.<ExternalLocationDAO>getEntitiesDAOsOverlapUrl(
            session,
            url,
            SecurableType.EXTERNAL_LOCATION,
            // If it needs to find *another* besides the current one, it has to find at least 2.
            /* limit= */ currentExternalLocationId.map(current -> 2).orElse(1),
            /* includeParent= */ true,
            /* includeSelf= */ true,
            /* includeSubdir= */ true)
        .stream()
        .filter(
            otherExternalLocation ->
                currentExternalLocationId
                    .map(id -> !id.equals(otherExternalLocation.getId()))
                    .orElse(true))
        .findAny()
        .ifPresent(
            otherExternalLocation -> {
              LOGGER.error(
                  "Cannot have external location with a URL {} that overlaps/duplicates with "
                      + "existing external location {} with url {}",
                  url,
                  otherExternalLocation.getName(),
                  otherExternalLocation.getUrl());
              throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT,
                  "Cannot accept an external location that duplicates or overlaps with existing "
                      + "external location");
            });
  }
}
