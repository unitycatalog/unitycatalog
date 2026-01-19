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
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.NormalizedURL;
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

  public ExternalLocationInfo addExternalLocation(CreateExternalLocation createExternalLocation) {
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

          NormalizedURL url = NormalizedURL.from(createExternalLocation.getUrl());
          validateUrlNotUsedByAnyExternalLocation(session, url, Optional.empty());

          CredentialDAO credentialDAO =
              validateAndGetCredentialDAO(session, createExternalLocation.getCredentialName());
          UUID externalLocationId = UUID.randomUUID();
          ExternalLocationDAO externalLocationDAO =
              ExternalLocationDAO.builder()
                  .id(externalLocationId)
                  .name(createExternalLocation.getName())
                  .url(url.toString())
                  .comment(createExternalLocation.getComment())
                  .owner(callerId)
                  .credentialId(credentialDAO.getId())
                  .createdAt(new Date())
                  .createdBy(callerId)
                  .build();
          session.persist(externalLocationDAO);
          LOGGER.info("External location added: {}", externalLocationDAO.getName());
          return externalLocationDAO.toExternalLocationInfo(credentialDAO.getName());
        },
        "Failed to add external location",
        /* readOnly = */ false);
  }

  public ExternalLocationInfo getExternalLocation(String name) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          ExternalLocationDAO externalLocationDAO = getExternalLocationDAO(session, name);
          if (externalLocationDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "External location not found: " + name);
          }
          LOGGER.debug("External location retrieved: {}", externalLocationDAO.getName());
          String credentialName =
              repositories
                  .getCredentialRepository()
                  .getCredentialName(session, externalLocationDAO.getCredentialId());
          return externalLocationDAO.toExternalLocationInfo(credentialName);
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
            String credentialName =
                repositories
                    .getCredentialRepository()
                    .getCredentialName(session, dao.getCredentialId());
            results.add(dao.toExternalLocationInfo(credentialName));
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
            NormalizedURL url = NormalizedURL.from(updateExternalLocation.getUrl());
            existingLocation.setUrl(url.toString());
            validateUrlNotUsedByAnyExternalLocation(
                session, url, Optional.of(existingLocation.getId()));
          }
          if (updateExternalLocation.getComment() != null) {
            existingLocation.setComment(updateExternalLocation.getComment());
          }
          String credentialName;
          if (updateExternalLocation.getCredentialName() != null) {
            // Setting a new credential
            credentialName = updateExternalLocation.getCredentialName();
            CredentialDAO credentialDAO = validateAndGetCredentialDAO(session, credentialName);
            existingLocation.setCredentialId(credentialDAO.getId());
          } else {
            // Keep the current credential
            credentialName =
                repositories
                    .getCredentialRepository()
                    .getCredentialName(session, existingLocation.getCredentialId());
          }

          existingLocation.setUpdatedAt(new Date());
          existingLocation.setUpdatedBy(callerId);

          session.merge(existingLocation);
          LOGGER.info("Updated external location: {}", name);
          return existingLocation.toExternalLocationInfo(credentialName);
        },
        "Failed to update external location",
        /* readOnly = */ false);
  }

  /**
   * Deletes an external location by name.
   *
   * <p>By default, deletion will fail if the external location is still in use by any tables,
   * volumes, or registered models. Use the force parameter to bypass this check and delete the
   * external location even if it is still referenced.
   *
   * @param name The name of the external location to delete
   * @param force If true, delete the external location even if it's still in use by other entities
   * @return The deleted ExternalLocationDAO
   * @throws BaseException with NOT_FOUND if the external location doesn't exist
   * @throws BaseException with INVALID_ARGUMENT if the external location is in use and force is
   *     false
   */
  public ExternalLocationDAO deleteExternalLocation(String name, boolean force) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          ExternalLocationDAO existingLocation = getExternalLocationDAO(session, name);
          if (existingLocation == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "External location not found: " + name);
          }
          // Check if the external location is in use by any data objects (tables, volumes, models)
          if (!force) {
            ExternalLocationUtils.getAllEntityDAOsWithURLOverlap(
                    session,
                    NormalizedURL.from(existingLocation.getUrl()),
                    ExternalLocationUtils.DATA_SECURABLE_TYPES,
                    /* limit= */ 1,
                    /* includeParent= */ false,
                    /* includeSelf= */ true,
                    /* includeSubdir= */ true)
                .stream()
                .findAny()
                .ifPresent(
                    pair -> {
                      throw new BaseException(
                          ErrorCode.INVALID_ARGUMENT,
                          String.format(
                              "External location still used by %s '%s'",
                              pair.getLeft(), pair.getRight().getId()));
                    });
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
      Session session, NormalizedURL url, Optional<UUID> currentExternalLocationId) {
    ExternalLocationUtils.<ExternalLocationDAO>getEntityDAOsWithURLOverlap(
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
