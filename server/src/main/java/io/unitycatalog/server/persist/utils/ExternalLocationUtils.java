package io.unitycatalog.server.persist.utils;

import com.google.common.annotations.VisibleForTesting;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.dao.IdentifiableDAO;
import io.unitycatalog.server.persist.dao.RegisteredModelInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
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

  private record DaoClassInfo(Class<? extends IdentifiableDAO> clazz, String urlFieldName) {}

  private static final Map<SecurableType, DaoClassInfo> SECURABLE_TYPE_TO_DAO_MAP =
      Map.of(
          SecurableType.TABLE, new DaoClassInfo(TableInfoDAO.class, "url"),
          SecurableType.VOLUME, new DaoClassInfo(VolumeInfoDAO.class, "storageLocation"),
          SecurableType.REGISTERED_MODEL, new DaoClassInfo(RegisteredModelInfoDAO.class, "url"),
          SecurableType.EXTERNAL_LOCATION, new DaoClassInfo(ExternalLocationDAO.class, "url"));

  /**
   * List of securable types that represent data objects (tables, volumes, registered models). Used
   * to check which entities are using an external location's URL path.
   */
  public static final List<SecurableType> DATA_SECURABLE_TYPES =
      List.of(SecurableType.TABLE, SecurableType.VOLUME, SecurableType.REGISTERED_MODEL);

  private static final List<SecurableType> EXTERNAL_LOCATION_AND_DATA_SECURABLE_TYPES =
      Stream.concat(Stream.of(SecurableType.EXTERNAL_LOCATION), DATA_SECURABLE_TYPES.stream())
          .toList();

  /**
   * For a input URL, find out the actual owner securables of the URL.
   *
   * <ul>
   *   <li>If the URL is a parent path of one or more securable, we can not figure out the actual
   *       owner but have to deny the access
   *   <li>If the URL is under or the same path of any data securable, we'll figure out the UUID of
   *       that data securable along with its catalog&schema UUIDs and return the result.
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
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> getMapResourceIdsForPath(session, url),
        "Failed to resolve resource IDs for path",
        /* readOnly= */ true);
  }

  private Map<SecurableType, UUID> getMapResourceIdsForPath(Session session, NormalizedURL url) {
    // 1. Fail if it's parent of any of the data securable or external location
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

    // 2. If it's under only one data securable, use that securable as resource id
    Optional<Map<SecurableType, UUID>> result =
        getResourceIdOfOwnerEntity(session, url, DATA_SECURABLE_TYPES);
    if (result.isPresent()) {
      return result.get();
    }

    // 3. If it's under only one external location, use that external location as resource id
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
      default -> throw new BaseException(
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

  /**
   * Finds the storage credential associated with the external location that covers the given path.
   *
   * <p>This method looks up external locations whose URL is a parent of or equals the given path,
   * then returns the storage credential associated with that external location. The credential can
   * be used to vend a temporary credential for data access.
   *
   * @param url the storage path to find credentials for (e.g., s3://bucket/path/to/data)
   * @return the storage credential if an external location covers this path, empty otherwise
   * @throws BaseException with FAILED_PRECONDITION if multiple external locations cover the path
   *     (invalid state) or if the credential referenced by the external location does not exist
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
          ErrorCode.FAILED_PRECONDITION,
          "More than one external location with URL '" + url + "' exist.");
    }

    // Get the credential that is assigned to the external location
    UUID credentialId = externalLocationDAOs.get(0).getCredentialId();
    CredentialDAO credentialDAO = session.get(CredentialDAO.class, credentialId);
    if (credentialDAO == null) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          String.format("Credential %s for '%s' not found.", credentialId, url));
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
    assert (includeParent || includeSelf || includeSubdir);
    DaoClassInfo daoClassInfo = SECURABLE_TYPE_TO_DAO_MAP.get(securableType);
    if (daoClassInfo == null) {
      throw new IllegalArgumentException(
          "Unsupported securable type for URL overlap check: " + securableType);
    }

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
      likePattern = escapedUrl + "/%";
      hasLikeCondition = true;
    }

    String inCondition = String.format("%s IN (:matchPaths)", daoClassInfo.urlFieldName);
    String likeCondition =
        String.format("%s LIKE :likePattern ESCAPE '\\'", daoClassInfo.urlFieldName);
    String condition = null;
    if (hasInCondition && hasLikeCondition) {
      condition = inCondition + " OR " + likeCondition;
    } else if (hasInCondition) {
      condition = inCondition;
    } else if (hasLikeCondition) {
      condition = likeCondition;
    }
    String queryString =
        String.format(
            "FROM %s WHERE %s ORDER BY LENGTH(%s) DESC",
            daoClassInfo.clazz.getSimpleName(), condition, daoClassInfo.urlFieldName);

    Query<T> query = session.createQuery(queryString, (Class<T>) daoClassInfo.clazz);
    if (!matchPaths.isEmpty()) {
      List<String> matchPathsList = matchPaths.stream().map(NormalizedURL::toString).toList();
      query.setParameter("matchPaths", matchPathsList);
    }
    if (!likePattern.isEmpty()) {
      query.setParameter("likePattern", likePattern);
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
}
