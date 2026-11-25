package io.unitycatalog.server.persist.utils;

import static io.unitycatalog.server.utils.Constants.MANAGED_STORAGE_CATALOG_PREFIX;
import static io.unitycatalog.server.utils.Constants.MANAGED_STORAGE_SCHEMA_PREFIX;

import com.google.common.annotations.VisibleForTesting;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.ManagedResourceType;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.dao.IdentifiableDAO;
import io.unitycatalog.server.persist.dao.MetastoreDAO;
import io.unitycatalog.server.persist.dao.RegisteredModelInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import io.unitycatalog.server.utils.ValidationUtils;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.hibernate.LockMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;

/**
 * Utility class for performing path-based database queries on Unity Catalog entities.
 *
 * <p>This class provides methods to find entities (tables, volumes, registered models, external
 * locations) whose URLs overlap with a given URL. URL overlap includes:
 *
 * <ul>
 *   <li>Exact match: The entity has the same URL
 *   <li>Parent match: The entity's URL is a parent directory of the given URL
 *   <li>Subdirectory match: The entity's URL is a subdirectory of the given URL
 * </ul>
 *
 * <p>This is particularly useful for validating that external locations do not overlap, as Unity
 * Catalog requires external locations to have non-overlapping URL hierarchies.
 *
 * <p>The class uses Hadoop's {@link org.apache.hadoop.fs.Path} for URI manipulation, which provides
 * correct handling of different storage schemes (file://, s3://, gs://, abfs://, etc.).
 */
public class ExternalLocationUtils {

  private final SessionFactory sessionFactory;

  public ExternalLocationUtils(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  /**
   * Internal record holding metadata about a DAO class for URL overlap queries.
   *
   * @param clazz the DAO class type
   * @param urlFieldName the name of the field containing the URL in the DAO
   * @param filterCondition additional HQL filter condition (empty string if none)
   */
  private record DaoClassInfo(
      Class<? extends IdentifiableDAO> clazz, String urlFieldName, String filterCondition) {
    DaoClassInfo(Class<? extends IdentifiableDAO> clazz, String urlFieldName) {
      this(clazz, urlFieldName, "");
    }
  }

  private static final Map<SecurableType, DaoClassInfo> SECURABLE_TYPE_TO_DAO_MAP =
      Map.of(
          SecurableType.TABLE, new DaoClassInfo(TableInfoDAO.class, "url"),
          SecurableType.VOLUME, new DaoClassInfo(VolumeInfoDAO.class, "storageLocation"),
          SecurableType.REGISTERED_MODEL, new DaoClassInfo(RegisteredModelInfoDAO.class, "url"),
          SecurableType.EXTERNAL_LOCATION, new DaoClassInfo(ExternalLocationDAO.class, "url"),
          SecurableType.CATALOG, new DaoClassInfo(CatalogInfoDAO.class, "storageLocation"),
          SecurableType.SCHEMA, new DaoClassInfo(SchemaInfoDAO.class, "storageLocation"));

  // Uncommitted staging tables does not have a corresponding SecurableType.
  @VisibleForTesting
  static final DaoClassInfo UNCOMMITTED_STAGING_TABLE_DAO_INFO =
      new DaoClassInfo(StagingTableDAO.class, "stagingLocation", "stageCommitted=false");

  // Cleanup tasks reserve paths but are not securables and cannot own path credentials.
  private static final DaoClassInfo STORAGE_CLEANUP_TASK_DAO_INFO =
      new DaoClassInfo(StorageCleanupTaskDAO.class, "storageLocation");

  private static final DaoClassInfo PENDING_CLEANUP_TASK_DAO_INFO =
      new DaoClassInfo(StorageCleanupTaskDAO.class, "storageLocation", "completedAt IS NULL");

  private static final DaoClassInfo REGISTERED_TABLE_RESERVATION_DAO_INFO =
      new DaoClassInfo(
          StorageCleanupTaskDAO.class,
          "storageLocation",
          "resourceType='%s'".formatted(ManagedResourceType.REGISTERED_TABLE.name()));

  private static final String MANAGED_ICEBERG_TABLE_FILTER =
      "type='%s' AND dataSourceFormat='%s'"
          .formatted(TableType.MANAGED.getValue(), DataSourceFormat.ICEBERG.getValue());

  /**
   * List of securable types that represent data objects (tables, volumes, registered models). Used
   * to check which entities are using an external location's URL path.
   */
  public static final List<SecurableType> DATA_SECURABLE_TYPES =
      List.of(SecurableType.TABLE, SecurableType.VOLUME, SecurableType.REGISTERED_MODEL);

  private static final List<SecurableType> EXTERNAL_LOCATION_AND_DATA_SECURABLE_TYPES =
      Stream.concat(Stream.of(SecurableType.EXTERNAL_LOCATION), DATA_SECURABLE_TYPES.stream())
          .toList();

  /** Catalogs and schemas are containers of managed securables. */
  private static final List<SecurableType> MANAGED_SECURABLE_TYPE_CONTAINERS =
      List.of(SecurableType.CATALOG, SecurableType.SCHEMA);

  private static final String FORCE_HINT = "Set force to true to skip this check.";

  /**
   * Securables that store data under an external location and get credentials from it. Uncommitted
   * staging tables are left out, unlike in the create-time overlap check, because orphaned staging
   * rows are never removed and would keep the location in use forever.
   */
  private static final List<SecurableType> STORAGE_USER_TYPES =
      Stream.concat(DATA_SECURABLE_TYPES.stream(), MANAGED_SECURABLE_TYPE_CONTAINERS.stream())
          .toList();

  /**
   * For a input URL, find out the actual owner securables of the URL.
   *
   * <ul>
   *   <li>If the URL overlaps a pending cleanup task, deny access.
   *   <li>If the URL is a parent path of one or more securable, we can not figure out the actual
   *       owner but have to deny the access
   *   <li>If the URL is under or the same path of any data securable, we'll figure out the UUID of
   *       that data securable along with its catalog and schema UUIDs and return the result.
   *   <li>If the URL is not owned by any data securable but only by external locations, return UUID
   *       of that external location.
   *   <li>Lastly if no securable is found, return empty map.
   * </ul>
   *
   * @param url the input URL to search securables for
   * @return A map of SecurableType->UUID. For external location, this will be a 1-entry map. For
   *     data securables, this will be a 3-entry map.
   */
  public Map<SecurableType, UUID> getMapResourceIdsForPath(NormalizedURL url) {
    return getMapResourceIdsForPath(url, /* isStorageRoot= */ false);
  }

  /** A namespace storage root allocates a separate directory; it does not grant path access. */
  public Map<SecurableType, UUID> getMapResourceIdsForPath(
      NormalizedURL url, boolean isStorageRoot) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> getMapResourceIdsForPath(session, url, isStorageRoot),
        "Failed to resolve resource IDs for path",
        /* readOnly= */ true);
  }

  @VisibleForTesting
  Map<SecurableType, UUID> getMapResourceIdsForPath(Session session, NormalizedURL url) {
    return getMapResourceIdsForPath(session, url, /* isStorageRoot= */ false);
  }

  private Map<SecurableType, UUID> getMapResourceIdsForPath(
      Session session, NormalizedURL url, boolean isStorageRoot) {
    validateCloudStoragePath(url);
    // 1. Cleanup tasks and completed registration cleanups reserve their paths.
    if (hasCleanupReservationOverlap(session, url, !isStorageRoot)) {
      throw new BaseException(
          ErrorCode.PERMISSION_DENIED,
          "Input path overlaps pending storage cleanup or a retired registered directory.");
    }

    // 2. Fail if it's parent of any of the data securable or external location
    if (!getAllEntityDAOsWithURLOverlap(
            session,
            url,
            EXTERNAL_LOCATION_AND_DATA_SECURABLE_TYPES,
            /* limit= */ 1,
            /* includeParent= */ false,
            /* includeSelf= */ false,
            /* includeSubdir= */ true)
        .isEmpty()) {
      throw new BaseException(
          ErrorCode.PERMISSION_DENIED, "Input path '" + url + "' overlaps with other entities.");
    }

    // 3. If it's under only one data securable, use that securable as resource id
    Optional<Map<SecurableType, UUID>> result =
        getResourceIdOfOwnerEntity(session, url, DATA_SECURABLE_TYPES);
    if (result.isPresent()) {
      return result.get();
    }

    // 4. If it's under only one external location, use that external location as resource id
    return getResourceIdOfOwnerEntity(session, url, List.of(SecurableType.EXTERNAL_LOCATION))
        .orElse(Map.of());
  }

  private Optional<Map<SecurableType, UUID>> getResourceIdOfOwnerEntity(
      Session session, NormalizedURL url, List<SecurableType> securableTypes) {
    List<Pair<SecurableType, IdentifiableDAO>> securablesContainUrl =
        getAllEntityDAOsWithURLOverlap(
            session,
            url,
            securableTypes,
            /* limit= */ 2,
            /* includeParent= */ true,
            /* includeSelf= */ true,
            /* includeSubdir= */ false);
    if (securablesContainUrl.size() > 1) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Input path '" + url + "' overlaps with multiple entities.");
    } else if (securablesContainUrl.isEmpty()) {
      return Optional.empty();
    }

    SecurableType securableType = securablesContainUrl.get(0).getLeft();
    IdentifiableDAO dao = securablesContainUrl.get(0).getRight();
    if (securableType == SecurableType.EXTERNAL_LOCATION) {
      return Optional.of(Map.of(securableType, dao.getId()));
    }

    UUID schemaId = getSchemaId(securableType, dao);
    SchemaInfoDAO schemaInfoDAO = session.get(SchemaInfoDAO.class, schemaId);
    if (schemaInfoDAO == null) {
      throw new BaseException(ErrorCode.INTERNAL, "Schema not found: " + schemaId);
    }
    UUID catalogId = schemaInfoDAO.getCatalogId();

    return Optional.of(
        Map.of(
            SecurableType.CATALOG,
            catalogId,
            SecurableType.SCHEMA,
            schemaId,
            securableType,
            dao.getId()));
  }

  private UUID getSchemaId(SecurableType securableType, IdentifiableDAO dao) {
    return switch (securableType) {
      case TABLE -> ((TableInfoDAO) dao).getSchemaId();
      case VOLUME -> ((VolumeInfoDAO) dao).getSchemaId();
      case REGISTERED_MODEL -> ((RegisteredModelInfoDAO) dao).getSchemaId();
      default ->
          throw new BaseException(
              ErrorCode.UNIMPLEMENTED, "Unknown securable type: " + securableType);
    };
  }

  /**
   * Finds all entities across multiple securable types whose URLs overlap with the given URL.
   *
   * <p>This is a convenience method that queries multiple securable types at once and returns
   * results as pairs of (SecurableType, IdentifiableDAO). Results are limited by 'limit' globally
   * across all types, not per type. For each SecurableType, multiple IdentifiableDAO objects can be
   * returned. For example, if it's called with securableTypes=[TABLE, VOLUME] and limit=3, and
   * there are a total of 2 matching tables and 2 matching volumes in the database, it returns:
   *
   * <p>[(TABLE, table1), (TABLE, table2), (VOLUME, volume1)] // volume2 isn't returned
   *
   * @param session The Hibernate session for database access
   * @param url The URL to check for overlaps
   * @param securableTypes List of securable types to search across (e.g., TABLE, VOLUME,
   *     REGISTERED_MODEL)
   * @param limit Maximum number of total results to return across all types
   * @param includeParent If true, include entities whose URL is a parent of the given URL
   * @param includeSelf If true, include entities with the exact same URL
   * @param includeSubdir If true, include entities whose URL is a subdirectory of the given URL
   * @return List of pairs containing the securable type and matching entity DAO, limited to the
   *     specified number of results
   * @throws IllegalArgumentException if any securableType is not supported for URL overlap checks
   */
  public static List<Pair<SecurableType, IdentifiableDAO>> getAllEntityDAOsWithURLOverlap(
      Session session,
      NormalizedURL url,
      List<SecurableType> securableTypes,
      int limit,
      boolean includeParent,
      boolean includeSelf,
      boolean includeSubdir) {
    // The flatMap().limit(limit) will stop executing next query once it finds enough entities.
    return securableTypes.stream()
        .flatMap(
            securableType ->
                generateEntitiesDAOsWithURLOverlapQuery(
                        session,
                        url,
                        securableType,
                        limit,
                        includeParent,
                        includeSelf,
                        includeSubdir)
                    .stream()
                    .map(entity -> Pair.<SecurableType, IdentifiableDAO>of(securableType, entity)))
        .limit(limit)
        .toList();
  }

  /** Returns the URL of every external location on the local file system, as stored. */
  public List<String> listLocalExternalLocationUrls() {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session ->
            session
                .createQuery(
                    "SELECT url FROM ExternalLocationDAO WHERE url LIKE 'file:%'", String.class)
                .list(),
        "Failed to list local external locations",
        /* readOnly= */ true);
  }

  /**
   * Finds the storage credential associated with the external location that covers the given path.
   *
   * <p>This method looks up external locations whose URL is a parent of or equals the given path,
   * then returns the storage credential associated with that external location. The credential can
   * be used to vend a temporary credential for data access.
   *
   * @param url the storage path to find credentials for (e.g., s3://bucket/path/to/data)
   * @return the storage credential if an external location covers this path, empty otherwise
   * @throws BaseException with INTERNAL if multiple external locations cover the path (invalid
   *     state), or if the credential referenced by the external location was deleted
   */
  public Optional<CredentialDAO> getExternalLocationCredentialDaoForPath(NormalizedURL url) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> getExternalLocationCredentialDaoForPath(session, url),
        "Failed to get storage credential by URL",
        /* readOnly= */ true);
  }

  private Optional<CredentialDAO> getExternalLocationCredentialDaoForPath(
      Session session, NormalizedURL url) {
    // Get the external location
    // Find out the external location that covers the input URL. Either the external location is
    // a parent path of input URL, or is the same URL.
    // It also tries to find two instead of one. If two are found, that is an invalid state that
    // should never happen and in that case server can't decide which external location to use.
    List<ExternalLocationDAO> externalLocationDAOs =
        ExternalLocationUtils.<ExternalLocationDAO>getEntityDAOsWithURLOverlap(
                session,
                url,
                SecurableType.EXTERNAL_LOCATION,
                /* limit= */ 2,
                /* includeParent= */ true,
                /* includeSelf= */ true,
                /* includeSubdir= */ false)
            .stream()
            .toList();
    if (externalLocationDAOs.isEmpty()) {
      // Not found
      return Optional.empty();
    } else if (externalLocationDAOs.size() > 1) {
      // This is an invalid internal state. We never allow external locations with
      // overlapping URLs.
      throw new BaseException(
          ErrorCode.INTERNAL, "More than one external location with URL '" + url + "' exist.");
    }

    // Get the credential that is assigned to the external location
    UUID credentialId = externalLocationDAOs.get(0).getCredentialId();
    CredentialDAO credentialDAO = session.get(CredentialDAO.class, credentialId);
    if (credentialDAO == null) {
      throw new BaseException(
          ErrorCode.INTERNAL,
          String.format(
              "The storage credential for '%s' no longer exists. Update the external location "
                  + "that covers this path with a new credential.",
              url));
    }
    return Optional.of(credentialDAO);
  }

  /**
   * Finds entities of the specified type whose URLs overlap with the given URL. Refer to
   * generateEntitiesDAOsWithURLOverlapQuery for the details.
   *
   * @param <T> The DAO type to return, must extend IdentifiableDAO
   * @param session The Hibernate session for database access
   * @param url The normalized URL to check for overlaps
   * @param securableType The type of securable entity to search (TABLE, VOLUME, REGISTERED_MODEL,
   *     EXTERNAL_LOCATION)
   * @param limit Maximum number of results to return
   * @param includeParent If true, include entities whose URL is a parent of the given URL
   * @param includeSelf If true, include entities with the exact same URL
   * @param includeSubdir If true, include entities whose URL is a subdirectory of the given URL
   * @return List of matching entity DAOs, ordered by URL length descending
   * @throws IllegalArgumentException if the securableType is not supported for URL overlap checks
   */
  public static <T extends IdentifiableDAO> List<T> getEntityDAOsWithURLOverlap(
      Session session,
      NormalizedURL url,
      SecurableType securableType,
      int limit,
      boolean includeParent,
      boolean includeSelf,
      boolean includeSubdir) {
    Query<T> query =
        generateEntitiesDAOsWithURLOverlapQuery(
            session, url, securableType, limit, includeParent, includeSelf, includeSubdir);
    return query.stream().toList();
  }

  /**
   * Generate a query to find entities of the specified securableType whose URLs overlap with the
   * given URL.
   *
   * <p>The query would search the database for entities that have URLs that overlap with the
   * provided URL. The type of overlap to check is controlled by the boolean flags:
   *
   * <ul>
   *   <li>{@code includeParent}: Include entities whose URL is a parent directory of the given URL
   *   <li>{@code includeSelf}: Include entities with the exact same URL
   *   <li>{@code includeSubdir}: Include entities whose URL is a subdirectory of the given URL
   * </ul>
   *
   * <p>Results are ordered by URL length (descending), so closer matches appear first.
   *
   * @param <T> The DAO type to return, must extend IdentifiableDAO
   * @param session The Hibernate session for database access
   * @param url The normalized URL to check for overlaps
   * @param securableType The type of securable entity to search (TABLE, VOLUME, REGISTERED_MODEL,
   *     EXTERNAL_LOCATION)
   * @param limit Maximum number of results to return
   * @param includeParent If true, include entities whose URL is a parent of the given URL
   * @param includeSelf If true, include entities with the exact same URL
   * @param includeSubdir If true, include entities whose URL is a subdirectory of the given URL
   * @return Query to find matching entity DAOs, ordered by URL length descending
   * @throws IllegalArgumentException if the securableType is not supported for URL overlap checks
   */
  @VisibleForTesting
  static <T extends IdentifiableDAO> Query<T> generateEntitiesDAOsWithURLOverlapQuery(
      Session session,
      NormalizedURL url,
      SecurableType securableType,
      int limit,
      boolean includeParent,
      boolean includeSelf,
      boolean includeSubdir) {
    DaoClassInfo daoClassInfo = SECURABLE_TYPE_TO_DAO_MAP.get(securableType);
    if (daoClassInfo == null) {
      throw new IllegalArgumentException(
          "Unsupported securable type for URL overlap check: " + securableType);
    }
    return generateEntitiesDAOsWithURLOverlapQuery(
        session, url, daoClassInfo, limit, includeParent, includeSelf, includeSubdir);
  }

  @VisibleForTesting
  static <T extends IdentifiableDAO> Query<T> generateEntitiesDAOsWithURLOverlapQuery(
      Session session,
      NormalizedURL url,
      DaoClassInfo daoClassInfo,
      int limit,
      boolean includeParent,
      boolean includeSelf,
      boolean includeSubdir) {
    assert (includeParent || includeSelf || includeSubdir);
    boolean hasInCondition = false;
    // parent paths + self
    List<NormalizedURL> matchPaths = includeParent ? getParentPathsList(url) : new ArrayList<>();
    if (includeSelf) {
      matchPaths.add(url);
    }
    if (!matchPaths.isEmpty()) {
      hasInCondition = true;
    }

    String likePattern = "";
    boolean hasLikeCondition = false;
    if (includeSubdir) {
      // Construct a LIKE pattern to match all child URLs. Escape special LIKE characters.
      String escapedUrl = escapeLikePattern(url.toString());
      // A filesystem root already ends in '/', but a child must add at least one character.
      likePattern = escapedUrl + (escapedUrl.endsWith("/") ? "_%" : "/%");
      hasLikeCondition = true;
    }

    String inCondition = String.format("%s IN (:matchPaths)", daoClassInfo.urlFieldName);
    String likeCondition =
        String.format("%s LIKE :likePattern ESCAPE '\\'", daoClassInfo.urlFieldName);
    List<NormalizedURL> spellings = storageLocationSpellings(url);
    if (includeSubdir && spellings.size() > 1) {
      likeCondition =
          "("
              + likeCondition
              + " OR "
              + daoClassInfo.urlFieldName
              + " LIKE :alternateLikePattern ESCAPE '\\')";
    }
    String condition = null;
    if (hasInCondition && hasLikeCondition) {
      condition = inCondition + " OR " + likeCondition;
    } else if (hasInCondition) {
      condition = inCondition;
    } else if (hasLikeCondition) {
      condition = likeCondition;
    } else {
      // This is not supposed to happen unless there's a bug.
      throw new RuntimeException("At least one condition must be provided");
    }

    if (!daoClassInfo.filterCondition.isEmpty()) {
      condition = String.format("%s AND (%s)", daoClassInfo.filterCondition, condition);
    }

    String queryString =
        String.format(
            "FROM %s WHERE %s ORDER BY LENGTH(%s) DESC",
            daoClassInfo.clazz.getSimpleName(), condition, daoClassInfo.urlFieldName);

    Query<T> query = session.createQuery(queryString, (Class<T>) daoClassInfo.clazz);
    if (!matchPaths.isEmpty()) {
      List<String> matchPathsList =
          matchPaths.stream()
              .flatMap(path -> storageLocationSpellings(path).stream())
              .map(NormalizedURL::toString)
              .toList();
      query.setParameter("matchPaths", matchPathsList);
    }
    if (!likePattern.isEmpty()) {
      query.setParameter("likePattern", likePattern);
      if (spellings.size() > 1) {
        query.setParameter(
            "alternateLikePattern", escapeLikePattern(spellings.get(1).toString()) + "/%");
      }
    }
    query.setMaxResults(limit);
    return query;
  }

  /**
   * Generates a list of all parent paths for a given URL, from immediate parent to root.
   *
   * <p>This method walks up the directory tree, collecting each parent URL until reaching the root.
   * For example, for "s3://bucket/a/b/c", it returns ["s3://bucket/a/b", "s3://bucket/a",
   * "s3://bucket"].
   *
   * <p>Special handling is applied for file:// URLs to ensure proper formatting (file:/// instead
   * of file:/), as Hadoop's Path class normalizes file URLs differently than other schemes.
   *
   * @param url The URL to extract parent paths from
   * @return List of parent paths from immediate parent to root, empty list if URL has no parent
   */
  @VisibleForTesting
  static List<NormalizedURL> getParentPathsList(NormalizedURL url) {
    List<NormalizedURL> parentPaths = new ArrayList<>();

    // Use Hadoop's Path class which handles URLs natively
    Path path = new Path(url.toString()).getParent();
    // Iterate from parent URL up to the root using getParent()
    while (path != null) {
      parentPaths.add(NormalizedURL.from(path.toString()));
      path = path.getParent();
    }
    return parentPaths;
  }

  /**
   * Escapes special LIKE pattern characters (% and _) in a string. Uses backslash as the escape
   * character.
   *
   * @param value The string to escape
   * @return The escaped string safe for use in LIKE patterns
   */
  @VisibleForTesting
  static String escapeLikePattern(String value) {
    // Escape backslash first, then % and _
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
  }

  /**
   * Validates that a URL does not contain the reserved managed storage prefix.
   *
   * <p>This check ensures that user-specified paths (such as external location URLs or storage
   * roots for catalogs/schemas) do not use or interfere with the reserved {@code __unitystorage}
   * namespace used for managed entities.
   *
   * @param url the URL to validate
   * @throws BaseException with ErrorCode.INVALID_ARGUMENT if the URL contains the managed storage
   *     prefix
   */
  public static void validateNotSameOrUnderManagedStoragePrefix(NormalizedURL url) {
    // url does not contain the reserved namespace `__unitystorage`
    if (url.toString().contains(Constants.MANAGED_STORAGE_PREFIX)) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          String.format(
              "Input path '%s' contains managed storage prefix %s.",
              url, Constants.MANAGED_STORAGE_PREFIX));
    }
  }

  /**
   * Validates that a URL is not a parent directory of any managed storage locations.
   *
   * <p>This prevents external locations from being created at paths that would encompass managed
   * catalog/schema storage locations or uncommitted staging tables.
   */
  private static void validateNotAboveManagedStorage(Session session, NormalizedURL url) {
    if (!getAllEntityDAOsWithURLOverlap(
            session,
            url,
            MANAGED_SECURABLE_TYPE_CONTAINERS,
            /* limit= */ 1,
            /* includeParent= */ false,
            /* includeSelf= */ false,
            /* includeSubdir= */ true)
        .isEmpty()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Input path '" + url + "' overlaps with managed storage.");
    }

    // volumes, committed tables, and models are already checked by KeyMapper. Uncommitted staging
    // tables aren't yet.
    // In normal cases the staging tables are under catalogs or schemas so the above check already
    // makes sure the input url doesn't cover any staging tables or tables. However, because the
    // staging tables are not deleted synchronously when the catalog/schema is deleted, there can be
    // orphaned uncommited staging tables remaining after the catalog/schema is gone. This check
    // covers the corner cases.
    if (generateEntitiesDAOsWithURLOverlapQuery(
            session,
            url,
            UNCOMMITTED_STAGING_TABLE_DAO_INFO,
            /* limit= */ 1,
            /* includeParent= */ false,
            /* includeSelf= */ false,
            /* includeSubdir= */ true)
        .stream()
        .findAny()
        .isPresent()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Input path '" + url + "' overlaps with staging table.");
    }
  }

  /**
   * Validates that a URL does not overlap with any managed storage locations.
   *
   * <p>This performs two checks:
   *
   * <ol>
   *   <li>The URL does not contain the {@code __unitystorage} prefix (same or under managed
   *       storage)
   *   <li>The URL is not a parent directory that would contain managed storage locations (above
   *       managed storage)
   * </ol>
   *
   * <p>This validation should be called when creating external entities (tables, volumes) to ensure
   * they don't interfere with managed storage.
   *
   * @param session the Hibernate session for database queries
   * @param url the URL to validate
   * @throws BaseException with ErrorCode.INVALID_ARGUMENT if the URL overlaps with managed storage
   */
  public static void validateNotOverlapWithManagedStorage(Session session, NormalizedURL url) {
    validateNotSameOrUnderManagedStoragePrefix(url);
    validateNotAboveManagedStorage(session, url);
  }

  /**
   * Validates that an external location at the given URL is not in use, so that removing the URL
   * from it does not leave anything without credentials. It is in use when:
   *
   * <ul>
   *   <li>a storage cleanup is pending above, at, or under the URL
   *   <li>a table, volume, or registered model is stored at or under the URL
   *   <li>a catalog or schema keeps its managed storage under the URL
   * </ul>
   *
   * @param session the caller's Hibernate session
   * @param url the URL of the external location
   * @throws BaseException with FAILED_PRECONDITION for a pending cleanup, INVALID_ARGUMENT
   *     otherwise
   */
  public static void validateExternalLocationNotInUse(Session session, NormalizedURL url) {
    LocationUse use = findLocationUse(session, url);
    if (use.pendingCleanup()) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "Input path overlaps pending storage cleanup. " + FORCE_HINT);
    }
    use.storageUser()
        .ifPresent(
            user -> {
              throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT,
                  "External location still used by " + user + ". " + FORCE_HINT);
            });
  }

  /**
   * What keeps an external location at a URL in use, as checked by {@link
   * #validateExternalLocationNotInUse}.
   *
   * @param pendingCleanup whether a storage cleanup is pending above, at, or under the URL
   * @param storageUser the first securable found that stores data at or under the URL, such as
   *     {@code volume '<id>'} or {@code a catalog}
   */
  public record LocationUse(boolean pendingCleanup, Optional<String> storageUser) {
    public boolean inUse() {
      return pendingCleanup || storageUser.isPresent();
    }
  }

  /**
   * Finds what keeps an external location at the URL in use, without throwing merely because the
   * location is in use.
   */
  public static LocationUse findLocationUse(Session session, NormalizedURL url) {
    return new LocationUse(hasPendingCleanupOverlap(session, url), findStorageUser(session, url));
  }

  private static Optional<String> findStorageUser(Session session, NormalizedURL url) {
    return getAllEntityDAOsWithURLOverlap(
            session,
            url,
            STORAGE_USER_TYPES,
            /* limit= */ 1,
            /* includeParent= */ false,
            /* includeSelf= */ true,
            /* includeSubdir= */ true)
        .stream()
        .findAny()
        .map(pair -> describeStorageUser(pair.getLeft(), pair.getRight().getId()));
  }

  /**
   * Catalogs and schemas are named by type only, so a caller who can manage the location but not
   * see the catalog or schema doesn't learn its ID.
   */
  private static String describeStorageUser(SecurableType type, UUID id) {
    return MANAGED_SECURABLE_TYPE_CONTAINERS.contains(type)
        ? "a " + type
        : String.format("%s '%s'", type, id);
  }

  /**
   * Returns whether an external location moved from {@code currentUrl} to {@code newUrl} still
   * covers everything under {@code currentUrl}, which holds when the new URL is the same or a
   * parent. A narrowed or unrelated URL does not, so the in-use check applies to it.
   */
  public static boolean stillCovers(NormalizedURL newUrl, NormalizedURL currentUrl) {
    List<NormalizedURL> spellings = storageLocationSpellings(newUrl);
    return spellings.contains(currentUrl)
        || getParentPathsList(currentUrl).stream().anyMatch(spellings::contains);
  }

  /** ADLS uses the same account, container, and path for both supported URI schemes. */
  private static List<NormalizedURL> storageLocationSpellings(NormalizedURL url) {
    String location = url.toString();
    if (location.startsWith("abfs://")) {
      return List.of(url, NormalizedURL.from("abfss" + location.substring("abfs".length())));
    }
    if (location.startsWith("abfss://")) {
      return List.of(url, NormalizedURL.from("abfs" + location.substring("abfss".length())));
    }
    return List.of(url);
  }

  /** Returns whether the parent paths of {@code url} can be built, which fails for s3:///. */
  public static boolean canBuildParentPaths(NormalizedURL url) {
    try {
      getParentPathsList(url);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Checks whether unfinished cleanup overlaps the given path.
   *
   * @param session the caller's Hibernate session
   * @param url the normalized path to check
   * @return whether unfinished cleanup overlaps the path
   */
  public static boolean hasPendingCleanupOverlap(Session session, NormalizedURL url) {
    return hasOverlap(session, url, PENDING_CLEANUP_TASK_DAO_INFO, true);
  }

  private static boolean hasCleanupReservationOverlap(
      Session session, NormalizedURL url, boolean includeSubdir) {
    return hasOverlap(session, url, STORAGE_CLEANUP_TASK_DAO_INFO, includeSubdir);
  }

  private static boolean hasRegisteredTableReservationOverlap(Session session, NormalizedURL url) {
    return hasOverlap(session, url, REGISTERED_TABLE_RESERVATION_DAO_INFO, true)
        || hasLocalStorageAlias(session, url, List.of(REGISTERED_TABLE_RESERVATION_DAO_INFO));
  }

  private static boolean hasOverlap(
      Session session, NormalizedURL url, DaoClassInfo type, boolean includeSubdir) {
    return !generateEntitiesDAOsWithURLOverlapQuery(
            session,
            url,
            type,
            /* limit= */ 1,
            /* includeParent= */ true,
            /* includeSelf= */ true,
            includeSubdir)
        .getResultList()
        .isEmpty();
  }

  /** A configured storage boundary for adopting an Iceberg table in place. */
  public record RegistrationStorage(NormalizedURL location, Optional<UUID> externalLocationId) {}

  /**
   * Resolves a path before reading registration metadata. Managed directories belonging to another
   * catalog or schema cannot be adopted through a covering external location.
   *
   * @param session the caller's transaction
   * @param parents the destination catalog and schema
   * @param path the metadata file or table directory
   * @param defaultRoot the server's managed table storage root, if configured
   * @param defaultModelRoot the server's managed model storage root, if configured
   * @return the storage boundary and, for external storage, the location to authorize
   * @throws BaseException if the path has no permitted storage boundary
   */
  public static RegistrationStorage resolveRegistrationStorage(
      Session session,
      RepositoryUtils.CatalogAndSchemaDao parents,
      NormalizedURL path,
      Optional<NormalizedURL> defaultRoot,
      Optional<NormalizedURL> defaultModelRoot) {
    LocalStorageLocationValidator.validateCanonicalLocalLocation(path);
    // Default roots have no namespace row. Keep their allocation directories reserved after
    // cleanup.
    if (Stream.concat(defaultRoot.stream(), defaultModelRoot.stream())
        .map(root -> NormalizedURL.from(root + "/" + Constants.MANAGED_STORAGE_PREFIX))
        .anyMatch(root -> stillCovers(path, root) || hasLocalStorageAlias(path, root.toString()))) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "The table path must not contain or equal a shared managed storage directory.");
    }
    validateNoLocalRegistrationAliases(session, path);
    validateRegistrationPathAvailable(session, path);
    var managedOwners =
        getAllEntityDAOsWithURLOverlap(
            session,
            path,
            MANAGED_SECURABLE_TYPE_CONTAINERS,
            Integer.MAX_VALUE,
            /* includeParent= */ true,
            /* includeSelf= */ true,
            /* includeSubdir= */ false);
    for (var owner : managedOwners) {
      UUID expected =
          owner.getLeft() == SecurableType.CATALOG
              ? parents.catalogInfoDAO().getId()
              : parents.schemaInfoDAO().getId();
      if (!expected.equals(owner.getRight().getId())) {
        throw new BaseException(
            ErrorCode.PERMISSION_DENIED,
            "The path is inside another catalog or schema's managed storage.");
      }
    }
    Optional<NormalizedURL> managedRoot =
        Stream.of(
                parents.schemaInfoDAO().getStorageLocation(),
                parents.catalogInfoDAO().getStorageLocation(),
                defaultRoot.map(root -> root + "/" + Constants.MANAGED_STORAGE_PREFIX).orElse(null))
            .filter(java.util.Objects::nonNull)
            .map(NormalizedURL::from)
            .filter(root -> isStrictSubpath(path, root))
            .max(Comparator.comparingInt(root -> root.toString().length()));
    if (managedRoot.isPresent()) {
      // ID-based directories can have old cleanup calls in flight after their task is removed.
      if (Stream.concat(
              Stream.of(
                      ManagedResourceType.TABLE,
                      ManagedResourceType.VOLUME,
                      ManagedResourceType.REGISTERED_MODEL)
                  .map(ManagedResourceType::pathSegment),
              Stream.of(
                  Constants.MANAGED_STORAGE_CATALOG_SEGMENT,
                  Constants.MANAGED_STORAGE_SCHEMA_SEGMENT))
          .map(segment -> NormalizedURL.from(managedRoot.get() + "/" + segment))
          .anyMatch(
              root -> stillCovers(root, path) || hasLocalStorageAlias(path, root.toString()))) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "The table path must not use a shared managed storage directory.");
      }
      return new RegistrationStorage(managedRoot.get(), Optional.empty());
    }
    validateNotSameOrUnderManagedStoragePrefix(path);
    List<ExternalLocationDAO> locations =
        getEntityDAOsWithURLOverlap(
            session,
            path,
            SecurableType.EXTERNAL_LOCATION,
            1,
            /* includeParent= */ true,
            /* includeSelf= */ false,
            /* includeSubdir= */ false);
    if (locations.isEmpty()) {
      throw new BaseException(
          ErrorCode.PERMISSION_DENIED,
          "The path must be inside an external location or the destination's managed storage"
              + " root.");
    }
    ExternalLocationDAO location = locations.get(0);
    return new RegistrationStorage(
        NormalizedURL.from(location.getUrl()), Optional.of(location.getId()));
  }

  /** Local filesystem aliases can overlap even when both URLs have canonical spelling. */
  private static void validateNoLocalRegistrationAliases(Session session, NormalizedURL path) {
    List<DaoClassInfo> types =
        Stream.concat(
                SECURABLE_TYPE_TO_DAO_MAP.values().stream(),
                Stream.of(UNCOMMITTED_STAGING_TABLE_DAO_INFO, STORAGE_CLEANUP_TASK_DAO_INFO))
            .toList();
    if (hasLocalStorageAlias(session, path, types)) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Registration path overlaps a stored local URL through a filesystem alias."
              + " Resolve the existing storage entry before registration.");
    }
  }

  private static boolean hasLocalStorageAlias(
      Session session, NormalizedURL path, List<DaoClassInfo> types) {
    if (UriScheme.fromURI(path.toUri()) != UriScheme.FILE) {
      return false;
    }
    for (DaoClassInfo type : types) {
      String filter = type.filterCondition.isEmpty() ? "" : type.filterCondition + " AND ";
      try (Stream<String> locations =
          session
              .createSelectionQuery(
                  "SELECT "
                      + type.urlFieldName
                      + " FROM "
                      + type.clazz.getSimpleName()
                      + " WHERE "
                      + filter
                      + type.urlFieldName
                      + " LIKE 'file:%'",
                  String.class)
              .getResultStream()) {
        if (locations.anyMatch(stored -> hasLocalStorageAlias(path, stored))) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean hasLocalStorageAlias(NormalizedURL path, String stored) {
    NormalizedURL storedUrl = NormalizedURL.from(stored);
    if (UriScheme.fromURI(path.toUri()) != UriScheme.FILE
        || UriScheme.fromURI(storedUrl.toUri()) != UriScheme.FILE) {
      return false;
    }
    // The SQL overlap checks already handle these spellings, including authorized containers.
    if (stillCovers(path, storedUrl) || stillCovers(storedUrl, path)) {
      return false;
    }
    return LocalStorageLocationValidator.storagePathsOverlap(
        Paths.get(path.toUri()), Paths.get(storedUrl.toUri()));
  }

  /** Rejects adoption of occupied, shared, or cleanup-reserved storage. */
  private static void validateRegistrationPathAvailable(Session session, NormalizedURL path) {
    if (!getAllEntityDAOsWithURLOverlap(
                session,
                path,
                DATA_SECURABLE_TYPES,
                1,
                /* includeParent= */ true,
                /* includeSelf= */ true,
                /* includeSubdir= */ true)
            .isEmpty()
        || !generateEntitiesDAOsWithURLOverlapQuery(
                session,
                path,
                UNCOMMITTED_STAGING_TABLE_DAO_INFO,
                1,
                /* includeParent= */ true,
                /* includeSelf= */ true,
                /* includeSubdir= */ true)
            .getResultList()
            .isEmpty()
        || hasCleanupReservationOverlap(session, path, true)) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "The path overlaps an existing table, volume, model, staging table, pending storage"
              + " cleanup, or a retired registered directory.");
    }
    if (!getAllEntityDAOsWithURLOverlap(
            session,
            path,
            List.of(SecurableType.CATALOG, SecurableType.SCHEMA, SecurableType.EXTERNAL_LOCATION),
            1,
            /* includeParent= */ false,
            /* includeSelf= */ true,
            /* includeSubdir= */ true)
        .isEmpty()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "The table path must not contain or equal a shared storage location.");
    }
  }

  /** Serializes storage allocation with table registration; use READ COMMITTED. */
  public static void lockStorageAllocation(Session session) {
    RepositoryUtils.unavailableOnLockFailure(
        "managed storage",
        () ->
            session
                .createSelectionQuery("FROM MetastoreDAO", MetastoreDAO.class)
                .setHibernateLockMode(LockMode.PESSIMISTIC_WRITE)
                .getSingleResult());
  }

  /** Prevents later creates from claiming an adopted directory, including after a drop. */
  public static void validateNotOverlapWithRegisteredTables(Session session, NormalizedURL path) {
    LocalStorageLocationValidator.validateCanonicalLocalLocation(path);
    validateCloudStoragePath(path);
    List<TableInfoDAO> overlaps =
        ExternalLocationUtils.<TableInfoDAO>generateEntitiesDAOsWithURLOverlapQuery(
                session,
                path,
                new DaoClassInfo(TableInfoDAO.class, "url", MANAGED_ICEBERG_TABLE_FILTER),
                Integer.MAX_VALUE,
                /* includeParent= */ true,
                /* includeSelf= */ true,
                /* includeSubdir= */ true)
            .getResultList();
    boolean hasLocalAlias = false;
    if (UriScheme.fromURI(path.toUri()) == UriScheme.FILE) {
      try (Stream<TableInfoDAO> localTables =
          session
              .createSelectionQuery(
                  "FROM TableInfoDAO WHERE "
                      + MANAGED_ICEBERG_TABLE_FILTER
                      + " AND url LIKE 'file:%'",
                  TableInfoDAO.class)
              .getResultStream()) {
        hasLocalAlias =
            localTables.anyMatch(
                table ->
                    ManagedResourceType.forTable(table) == ManagedResourceType.REGISTERED_TABLE
                        && hasLocalStorageAlias(path, table.getUrl()));
      }
    }
    if (hasRegisteredTableReservationOverlap(session, path)
        || hasLocalAlias
        || overlaps.stream()
            .anyMatch(
                table ->
                    ManagedResourceType.forTable(table) == ManagedResourceType.REGISTERED_TABLE)) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "The path overlaps a registered managed table, its pending storage cleanup,"
              + " or its retired directory.");
    }
  }

  /**
   * Checks strict containment, including equivalent Azure schemes.
   *
   * @param path the candidate child location
   * @param root the containing directory
   * @return whether path is below root and does not name the same location
   */
  public static boolean isStrictSubpath(NormalizedURL path, NormalizedURL root) {
    return stillCovers(root, path) && !stillCovers(path, root);
  }

  /**
   * Rejects cloud credential roots whose decoded path differs from the checked object prefix.
   *
   * @param path the storage allocation or credential root
   * @throws BaseException with INVALID_ARGUMENT if a cloud path contains escapes, a query, or a
   *     fragment
   */
  public static void validateCloudStoragePath(NormalizedURL path) {
    UriScheme scheme = UriScheme.fromURI(path.toUri());
    if (scheme != UriScheme.FILE) {
      // Credential scopes decode URI paths. The checked prefix must keep the same object key.
      ValidationUtils.checkArgument(
          !path.toString().contains("%")
              && path.toUri().getRawQuery() == null
              && path.toUri().getRawFragment() == null,
          "Cloud storage path ('%s') must not contain percent escapes, a query, or a fragment.",
          path);
    }
  }

  /**
   * Gets the managed storage location for creating child entities (tables, volumes, models) within
   * a catalog/schema.
   *
   * <p>This method returns the storageLocation of the schema if set, otherwise falls back to the
   * catalog's storageLocation. If neither is set, throws an exception.
   *
   * @param catalogAndSchemaDao the catalog and schema DAOs
   * @return the storage location to use as root for child entity storage
   * @throws BaseException with ErrorCode.FAILED_PRECONDITION if neither catalog nor schema has
   *     managed location configured
   */
  public static NormalizedURL getManagedStorageLocation(
      RepositoryUtils.CatalogAndSchemaDao catalogAndSchemaDao) {
    return getManagedStorageLocation(catalogAndSchemaDao, Optional::empty);
  }

  /**
   * Gets the managed storage location for creating child entities (tables, volumes, models) within
   * a catalog/schema, with a fallback option.
   *
   * <p>The resolution order is:
   *
   * <ol>
   *   <li>Schema's storageLocation (if set)
   *   <li>Catalog's storageLocation (if set)
   *   <li>Fallback storage root with __unitystorage prefix added (if provided and present)
   *   <li>Throws exception if none of the above are available
   * </ol>
   *
   * @param catalogAndSchemaDao the catalog and schema DAOs
   * @param fallbackStorageRoot supplier that provides an optional fallback storage root from server
   *     properties
   * @return the storage location to use as root for child entity storage
   * @throws BaseException with ErrorCode.FAILED_PRECONDITION if no storage root is available
   */
  public static NormalizedURL getManagedStorageLocation(
      RepositoryUtils.CatalogAndSchemaDao catalogAndSchemaDao,
      Supplier<Optional<NormalizedURL>> fallbackStorageRoot) {
    CatalogInfoDAO catalogInfoDAO = catalogAndSchemaDao.catalogInfoDAO();
    SchemaInfoDAO schemaInfoDAO = catalogAndSchemaDao.schemaInfoDAO();
    if (schemaInfoDAO.getStorageLocation() != null) {
      return NormalizedURL.from(schemaInfoDAO.getStorageLocation());
    } else if (catalogInfoDAO.getStorageLocation() != null) {
      return NormalizedURL.from(catalogInfoDAO.getStorageLocation());
    } else {
      return fallbackStorageRoot
          .get()
          .map(fallback -> NormalizedURL.from(fallback + "/" + Constants.MANAGED_STORAGE_PREFIX))
          .orElseThrow(
              () ->
                  new BaseException(
                      ErrorCode.FAILED_PRECONDITION,
                      "None of catalog, schema or storage-root.tables server property has managed "
                          + "location configured."));
    }
  }

  private static NormalizedURL getManagedLocationForEntity(
      NormalizedURL storageRoot, String prefix, UUID entityId) {
    return NormalizedURL.from(
        String.join("/", List.of(storageRoot.toString(), prefix, entityId.toString())));
  }

  public static NormalizedURL getManagedLocationForSchema(
      NormalizedURL storageRoot, UUID schemaId) {
    return getManagedLocationForEntity(storageRoot, MANAGED_STORAGE_SCHEMA_PREFIX, schemaId);
  }

  public static NormalizedURL getManagedLocationForCatalog(
      NormalizedURL storageRoot, UUID catalogId) {
    return getManagedLocationForEntity(storageRoot, MANAGED_STORAGE_CATALOG_PREFIX, catalogId);
  }

  // The following methods do not add a __unitystorage prefix because the storageRoot
  // is expected to be the storageLocation of a catalog or schema, which already includes
  // the __unitystorage prefix.

  public static NormalizedURL getManagedLocationForTable(
      NormalizedURL parentStorageLocation, UUID tableId) {
    return getManagedLocationForEntity(
        parentStorageLocation, ManagedResourceType.TABLE.pathSegment(), tableId);
  }

  public static NormalizedURL getManagedLocationForVolume(
      NormalizedURL parentStorageLocation, UUID volumeId) {
    return getManagedLocationForEntity(
        parentStorageLocation, ManagedResourceType.VOLUME.pathSegment(), volumeId);
  }

  public static NormalizedURL getManagedLocationForModel(
      NormalizedURL parentStorageLocation, UUID modelId) {
    return getManagedLocationForEntity(
        parentStorageLocation, ManagedResourceType.REGISTERED_MODEL.pathSegment(), modelId);
  }

  public static NormalizedURL getManagedLocationForModelVersion(
      NormalizedURL parentModelStorageLocation, UUID modelVersionId) {
    return getManagedLocationForEntity(
        parentModelStorageLocation,
        ManagedResourceType.MODEL_VERSION.pathSegment(),
        modelVersionId);
  }
}
