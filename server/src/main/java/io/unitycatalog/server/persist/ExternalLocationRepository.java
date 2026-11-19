package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateExternalLocation;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.model.ExternalLocationInfo;
import io.unitycatalog.server.model.ListExternalLocationsResponse;
import io.unitycatalog.server.model.S3AddressingStyle;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.UpdateExternalLocation;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.CredentialDAO.CredentialType;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ValidationUtils;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
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
    // Locked, so a concurrent change of the credential's type waits for this check, or this one for
    // that change (see CredentialRepository#getCredentialDAOForUpdate).
    CredentialDAO credentialDAO =
        repositories.getCredentialRepository().getCredentialDAOForUpdate(session, credentialName);
    if (credentialDAO == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Credential not found: " + credentialName);
    }
    if (!credentialDAO.getPurpose().equals(CredentialPurpose.STORAGE)) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Credential not of type storage: " + credentialName);
    }
    return credentialDAO;
  }

  // A bucket name that is also a host name label, which virtual-hosted requests put in the host;
  // no dots, since a dot breaks wildcard TLS certificates.
  private static final Pattern VIRTUAL_HOSTED_BUCKET =
      Pattern.compile("[a-z0-9][a-z0-9-]{1,61}[a-z0-9]");

  /**
   * Checks that a location can be reached with its credential: an {@code s3_compatible_access_key}
   * credential reaches S3-compatible storage, so its locations must be {@code s3://} paths with the
   * storage's endpoint and addressing style, and other credentials' locations have neither.
   *
   * @param url the location URL
   * @param endpoint the location's endpoint, or null if it has none
   * @param addressingStyle the location's addressing style, or null if it has none
   * @param credentialDAO the location's storage credential, or null if it was force-deleted
   * @throws BaseException with INVALID_ARGUMENT if the credential cannot reach the URL
   */
  static void validateForCredential(
      String url, String endpoint, S3AddressingStyle addressingStyle, CredentialDAO credentialDAO) {
    // A force-deleted credential leaves nothing to check against; setting a new one checks again.
    if (credentialDAO == null) {
      return;
    }
    if (credentialDAO.getCredentialType() != CredentialType.S3_COMPATIBLE) {
      ValidationUtils.checkArgument(
          endpoint == null && addressingStyle == null,
          "Storage credential '%s' is not an s3_compatible_access_key credential, so its external"
              + " locations have no endpoint or addressing style.",
          credentialDAO.getName());
      return;
    }
    // Only an s3_compatible_access_key credential limits its locations' URLs, so a location an
    // earlier release stored with a URL that no longer parses can still change otherwise; such a
    // URL is not an s3:// path.
    ValidationUtils.checkArgument(
        NormalizedURL.isS3Path(url),
        "Storage credential '%s' is an s3_compatible_access_key credential, so its external"
            + " locations must be s3:// paths: %s",
        credentialDAO.getName(),
        url);
    ValidationUtils.checkArgument(
        endpoint != null,
        "Storage credential '%s' is an s3_compatible_access_key credential, so its external"
            + " locations need the endpoint of their storage.",
        credentialDAO.getName());
    ValidationUtils.validateHttpUrl(endpoint, "endpoint");
    String bucket = NormalizedURL.from(url).toUri().getRawAuthority();
    ValidationUtils.checkArgument(
        addressingStyle != S3AddressingStyle.VIRTUAL_HOSTED
            || VIRTUAL_HOSTED_BUCKET.matcher(bucket).matches(),
        "Bucket '%s' cannot be addressed VIRTUAL_HOSTED: use 3 to 63 lowercase letters, digits and"
            + " hyphens, starting and ending with a letter or digit, or the PATH addressing style.",
        bucket);
  }

  public ExternalLocationInfo addExternalLocation(CreateExternalLocation createExternalLocation) {
    ValidationUtils.validateSqlObjectName(createExternalLocation.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          if (getExternalLocationDAO(session, createExternalLocation.getName()) != null) {
            throw new BaseException(
                ErrorCode.EXTERNAL_LOCATION_ALREADY_EXISTS,
                "External location already exists: " + createExternalLocation.getName());
          }

          NormalizedURL url = NormalizedURL.from(createExternalLocation.getUrl());
          ExternalLocationUtils.validateNotSameOrUnderManagedStoragePrefix(url);
          validateUrlNotUsedByAnyExternalLocation(session, url, Optional.empty());

          CredentialDAO credentialDAO =
              validateAndGetCredentialDAO(session, createExternalLocation.getCredentialName());
          // A location with an endpoint is addressed path-style unless it says otherwise.
          S3AddressingStyle addressingStyle =
              createExternalLocation.getAddressingStyle() == null
                      && createExternalLocation.getEndpoint() != null
                  ? S3AddressingStyle.PATH
                  : createExternalLocation.getAddressingStyle();
          validateForCredential(
              url.toString(), createExternalLocation.getEndpoint(), addressingStyle, credentialDAO);
          UUID externalLocationId = UUID.randomUUID();
          ExternalLocationDAO externalLocationDAO =
              ExternalLocationDAO.builder()
                  .id(externalLocationId)
                  .name(createExternalLocation.getName())
                  .url(url.toString())
                  .comment(createExternalLocation.getComment())
                  .owner(callerId)
                  .credentialId(credentialDAO.getId())
                  .endpoint(createExternalLocation.getEndpoint())
                  .addressingStyle(addressingStyle == null ? null : addressingStyle.getValue())
                  .createdAt(new Date())
                  .createdBy(callerId)
                  .build();
          session.persist(externalLocationDAO);
          LOGGER.info("External location added: {}", externalLocationDAO.getName());
          return externalLocationDAO.toExternalLocationInfo(credentialDAO.getName());
        },
        "Failed to add external location",
        /* readOnly= */ false,
        // Reads after the row lock see what was committed while it waited (see
        // RepositoryUtils#getForUpdateByName).
        Optional.of(Connection.TRANSACTION_READ_COMMITTED));
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
        /* readOnly= */ true);
  }

  protected ExternalLocationDAO getExternalLocationDAO(Session session, String name) {
    Query<ExternalLocationDAO> query =
        session.createQuery(
            "FROM ExternalLocationDAO WHERE name = :value", ExternalLocationDAO.class);
    query.setParameter("value", name);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  /**
   * Reads the location with a write lock on its row, so that concurrent updates of it take turns
   * and none writes back a stale copy over another's change.
   *
   * @return the location, or null if none has the name
   */
  ExternalLocationDAO getExternalLocationDAOForUpdate(Session session, String name) {
    return RepositoryUtils.getForUpdateByName(session, ExternalLocationDAO.class, name);
  }

  public ListExternalLocationsResponse listExternalLocations(
      Optional<Integer> maxResults, Optional<String> pageToken) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          List<ExternalLocationDAO> daoList =
              LISTING_HELPER.listEntity(session, maxResults, pageToken, /* parentEntityId= */ null);
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
        /* readOnly= */ true);
  }

  /**
   * Updates an external location by name.
   *
   * <p>Changing the URL fails while the external location is in use, as described for {@link
   * #deleteExternalLocation}, unless the new URL is the same as, or a parent of, the current one,
   * so everything under the current URL keeps getting credentials from it. Set force to change the
   * URL anyway. A forced change of a location in use is logged as a warning.
   *
   * @param name The name of the external location to update
   * @param updateExternalLocation The fields to change
   * @return The updated external location
   * @throws BaseException with NOT_FOUND if the external location doesn't exist
   * @throws BaseException with INVALID_ARGUMENT if the URL change is refused because the location
   *     is in use, or the new URL is invalid or overlaps another external location
   * @throws BaseException with FAILED_PRECONDITION if the URL change is refused because a storage
   *     cleanup is pending at, above, or under the current URL
   */
  public ExternalLocationInfo updateExternalLocation(
      String name, UpdateExternalLocation updateExternalLocation) {
    String callerId = IdentityUtils.findPrincipalEmailAddress();

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          // The location, then its credential, are locked on their first read in this session.
          ExternalLocationDAO existingLocation = getExternalLocationDAOForUpdate(session, name);
          if (existingLocation == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "External location not found: " + name);
          }

          // Update fields if provided
          if (updateExternalLocation.getNewName() != null) {
            ValidationUtils.validateSqlObjectName(updateExternalLocation.getNewName());
            if (getExternalLocationDAO(session, updateExternalLocation.getNewName()) != null) {
              throw new BaseException(
                  ErrorCode.EXTERNAL_LOCATION_ALREADY_EXISTS,
                  "External location already exists: " + updateExternalLocation.getNewName());
            }
            existingLocation.setName(updateExternalLocation.getNewName());
          }
          if (updateExternalLocation.getUrl() != null) {
            NormalizedURL url = NormalizedURL.from(updateExternalLocation.getUrl());
            ExternalLocationUtils.validateNotSameOrUnderManagedStoragePrefix(url);
            validateUrlNotUsedByAnyExternalLocation(
                session, url, Optional.of(existingLocation.getId()));
            if (Boolean.TRUE.equals(updateExternalLocation.getForce())) {
              warnIfInUse(
                  session,
                  existingLocation.getUrl(),
                  Optional.of(url),
                  "Changing the URL of external location '" + name + "' to " + url);
            } else {
              NormalizedURL currentUrl = storedUrlToCheckOrRefuse(existingLocation.getUrl(), name);
              if (!ExternalLocationUtils.stillCovers(url, currentUrl)) {
                ExternalLocationUtils.validateExternalLocationNotInUse(session, currentUrl);
              }
            }
            existingLocation.setUrl(url.toString());
          }
          if (updateExternalLocation.getComment() != null) {
            existingLocation.setComment(updateExternalLocation.getComment());
          }
          if (updateExternalLocation.getEndpoint() != null) {
            existingLocation.setEndpoint(updateExternalLocation.getEndpoint());
          }
          if (updateExternalLocation.getAddressingStyle() != null) {
            existingLocation.setAddressingStyle(
                updateExternalLocation.getAddressingStyle().getValue());
          }
          String credentialName;
          if (updateExternalLocation.getCredentialName() != null) {
            // Setting a new credential
            credentialName = updateExternalLocation.getCredentialName();
            CredentialDAO credentialDAO = validateAndGetCredentialDAO(session, credentialName);
            validateTypeKept(
                session.get(CredentialDAO.class, existingLocation.getCredentialId()),
                credentialDAO);
            existingLocation.setCredentialId(credentialDAO.getId());
            validateForCredential(
                existingLocation.getUrl(),
                existingLocation.getEndpoint(),
                storedAddressingStyle(existingLocation),
                credentialDAO);
          } else {
            // Keep the current credential. Locked on its first read in this session: a lock taken
            // on an entity already loaded does not refresh it, so a concurrent type change could go
            // unseen. Null if the credential was force-deleted.
            CredentialDAO current =
                RepositoryUtils.getForUpdate(
                    session, CredentialDAO.class, existingLocation.getCredentialId());
            credentialName = current == null ? null : current.getName();
            // Checked whether or not the URL changes, against the credential's committed type.
            validateForCredential(
                existingLocation.getUrl(),
                existingLocation.getEndpoint(),
                storedAddressingStyle(existingLocation),
                current);
          }

          existingLocation.setUpdatedAt(new Date());
          existingLocation.setUpdatedBy(callerId);

          session.merge(existingLocation);
          LOGGER.info("Updated external location: {}", name);
          return existingLocation.toExternalLocationInfo(credentialName);
        },
        "Failed to update external location",
        /* readOnly= */ false,
        // Reads after the row lock see what was committed while it waited (see
        // RepositoryUtils#getForUpdateByName).
        Optional.of(Connection.TRANSACTION_READ_COMMITTED));
  }

  /** The addressing style the location stores, or null if none, without the default. */
  private static S3AddressingStyle storedAddressingStyle(ExternalLocationDAO location) {
    return location.getAddressingStyle() == null
        ? null
        : S3AddressingStyle.fromValue(location.getAddressingStyle());
  }

  /**
   * Rejects moving a location between an {@code s3_compatible_access_key} credential and a
   * credential of another type, since the location has an endpoint exactly with the former.
   *
   * @param current the location's credential, or null if it was force-deleted
   * @param next the credential the update sets
   */
  private static void validateTypeKept(CredentialDAO current, CredentialDAO next) {
    ValidationUtils.checkArgument(
        current == null || current.getCredentialType() == next.getCredentialType(),
        "An external location cannot move between an s3_compatible_access_key credential and a"
            + " credential of another type: %s",
        next.getName());
  }

  /**
   * Deletes an external location by name.
   *
   * <p>By default, deletion will fail if the external location is still in use by any tables,
   * volumes, registered models, managed storage of catalogs or schemas, or pending storage cleanup.
   * Use the force parameter to bypass this check and delete the external location even if it is
   * still referenced.
   *
   * @param name The name of the external location to delete
   * @param force If true, delete the external location even if it's still in use by other entities
   * @return The deleted ExternalLocationDAO
   * @throws BaseException with NOT_FOUND if the external location doesn't exist
   * @throws BaseException with INVALID_ARGUMENT if the external location is in use and force is
   *     false
   * @throws BaseException with FAILED_PRECONDITION if a storage cleanup is pending at, above, or
   *     under the external location's URL and force is false
   */
  public ExternalLocationDAO deleteExternalLocation(String name, boolean force) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          ExternalLocationDAO existingLocation = getExternalLocationDAO(session, name);
          if (existingLocation == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "External location not found: " + name);
          }
          if (force) {
            warnIfInUse(
                session,
                existingLocation.getUrl(),
                Optional.empty(),
                "Deleting external location '" + name + "'");
          } else {
            ExternalLocationUtils.validateExternalLocationNotInUse(
                session, storedUrlToCheckOrRefuse(existingLocation.getUrl(), name));
          }
          session.remove(existingLocation);
          LOGGER.info("Deleted external location: {}", name);
          return existingLocation;
        },
        "Failed to delete external location",
        /* readOnly= */ false);
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

  /**
   * Logs a warning when a forced delete, or a forced change to {@code newUrl}, leaves the external
   * location stored at {@code storedUrl} in use. This only logs, so a URL stored by an earlier
   * release that no longer parses doesn't block the forced change.
   */
  private static void warnIfInUse(
      Session session, String storedUrl, Optional<NormalizedURL> newUrl, String action) {
    Optional<NormalizedURL> currentUrl = storedUrlToCheck(storedUrl);
    if (currentUrl.isEmpty()) {
      LOGGER.warn("{} with force. Its stored URL cannot be checked, as it does not parse.", action);
      return;
    }
    if (newUrl.isPresent() && ExternalLocationUtils.stillCovers(newUrl.get(), currentUrl.get())) {
      return;
    }
    ExternalLocationUtils.LocationUse use =
        ExternalLocationUtils.findLocationUse(session, currentUrl.get());
    if (use.inUse()) {
      LOGGER.warn(
          "{} with force while it is in use. Pending storage cleanup: {}. Still used by: {}.",
          action,
          use.pendingCleanup(),
          use.storageUser().orElse("nothing"));
    }
  }

  /**
   * Parses a stored URL for the in-use check. Empty when an earlier release stored a URL that the
   * current parser rejects, or one whose parent paths can't be built, such as s3:///.
   */
  private static Optional<NormalizedURL> storedUrlToCheck(String storedUrl) {
    try {
      NormalizedURL url = NormalizedURL.from(storedUrl);
      return ExternalLocationUtils.canBuildParentPaths(url) ? Optional.of(url) : Optional.empty();
    } catch (BaseException | IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  /** The error leaves the stored URL out, since the caller did not send it. */
  private static NormalizedURL storedUrlToCheckOrRefuse(String storedUrl, String name) {
    return storedUrlToCheck(storedUrl)
        .orElseThrow(
            () ->
                new BaseException(
                    ErrorCode.INVALID_ARGUMENT,
                    "The stored URL of external location '"
                        + name
                        + "' cannot be checked for use, as it does not parse. Set force to true"
                        + " to skip this check."));
  }
}
