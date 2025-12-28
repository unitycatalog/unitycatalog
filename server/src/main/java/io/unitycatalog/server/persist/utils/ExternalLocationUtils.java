package io.unitycatalog.server.persist.utils;

import com.google.common.annotations.VisibleForTesting;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.dao.IdentifiableDAO;
import io.unitycatalog.server.persist.dao.RegisteredModelInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.utils.Constants;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.hibernate.Session;
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

  private ExternalLocationUtils() {}

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

  /**
   * Finds all entities across multiple securable types whose URLs overlap with the given URL.
   *
   * <p>This is a convenience method that queries multiple securable types at once and returns
   * results as pairs of (SecurableType, IdentifiableDAO). Results are limited by 'limit' globally
   * across all types, not per type.
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
  public static List<Pair<SecurableType, IdentifiableDAO>> getAllEntityDAOsOverlapUrl(
      Session session,
      String url,
      List<SecurableType> securableTypes,
      int limit,
      boolean includeParent,
      boolean includeSelf,
      boolean includeSubdir) {
    // The flatMap().limit(limit) will stop executing next query once it finds enough entities.
    return securableTypes.stream()
        .flatMap(
            securableType ->
                generateEntitiesDAOsOverlapUrlQuery(
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
   * Finds entities of the specified type whose URLs overlap with the given URL. Refer to
   * generateEntitiesDAOsOverlapUrlQuery for the details.
   *
   * @param <T> The DAO type to return, must extend IdentifiableDAO
   * @param session The Hibernate session for database access
   * @param url The URL to check for overlaps (should be standardized via {@link
   *     FileOperations#toStandardizedURIString})
   * @param securableType The type of securable entity to search (TABLE, VOLUME, REGISTERED_MODEL,
   *     EXTERNAL_LOCATION)
   * @param limit Maximum number of results to return
   * @param includeParent If true, include entities whose URL is a parent of the given URL
   * @param includeSelf If true, include entities with the exact same URL
   * @param includeSubdir If true, include entities whose URL is a subdirectory of the given URL
   * @return List of matching entity DAOs, ordered by URL length descending
   * @throws IllegalArgumentException if the securableType is not supported for URL overlap checks
   */
  public static <T extends IdentifiableDAO> List<T> getEntitiesDAOsOverlapUrl(
      Session session,
      String url,
      SecurableType securableType,
      int limit,
      boolean includeParent,
      boolean includeSelf,
      boolean includeSubdir) {
    Query<T> query =
        generateEntitiesDAOsOverlapUrlQuery(
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
   * @param url The URL to check for overlaps (should be standardized via {@link
   *     FileOperations#toStandardizedURIString})
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
  static <T extends IdentifiableDAO> Query<T> generateEntitiesDAOsOverlapUrlQuery(
      Session session,
      String url,
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
    List<String> matchPaths = includeParent ? getParentPathsList(url) : new ArrayList<>();
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
      String normalizedUrl = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
      String escapedUrl = escapeLikePattern(normalizedUrl);
      likePattern = escapedUrl + "/%";
      hasLikeCondition = true;
    }

    String inConditon = String.format("%s IN (:matchPaths)", daoClassInfo.urlFieldName);
    String likeConditon =
        String.format("%s LIKE :likePattern ESCAPE '\\'", daoClassInfo.urlFieldName);
    String condition = null;
    if (hasInCondition && hasLikeCondition) {
      condition = inConditon + " OR " + likeConditon;
    } else if (hasInCondition) {
      condition = inConditon;
    } else if (hasLikeCondition) {
      condition = likeConditon;
    }
    String queryString =
        String.format(
            "FROM %s WHERE %s ORDER BY LENGTH(%s) DESC",
            daoClassInfo.clazz.getSimpleName(), condition, daoClassInfo.urlFieldName);

    Query<T> query = session.createQuery(queryString, (Class<T>) daoClassInfo.clazz);
    if (!matchPaths.isEmpty()) {
      query.setParameter("matchPaths", matchPaths);
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
  static List<String> getParentPathsList(String url) {
    List<String> parentPaths = new ArrayList<>();

    // Use Hadoop's Path class which handles URLs natively
    Path path = new Path(FileOperations.toStandardizedURIString(url)).getParent();
    // Iterate from parent URL up to the root using getParent()
    while (path != null) {
      URI uri = path.toUri();
      if (uri.getScheme().equals(Constants.URI_SCHEME_FILE)) {
        // Hadoop's Path normalizes file:/// to file:/, localFileURIToString fixes it back
        parentPaths.add(FileOperations.localFileURIToString(uri));
      } else {
        parentPaths.add(FileOperations.removeExtraSlashes(uri.toString()));
      }
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
