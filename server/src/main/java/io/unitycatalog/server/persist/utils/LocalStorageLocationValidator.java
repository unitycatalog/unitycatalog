package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.UriScheme;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Decides whether the server may create files at a caller-chosen local location for an external
 * securable.
 *
 * <p>The server writes local files with its own OS identity, and authorization checks no privilege
 * for a path outside every external location. So a local location is allowed only when it is
 *
 * <ul>
 *   <li>under an external location (authorization already checked the caller's privilege), or
 *   <li>strictly under a root in {@code server.external-local-roots}.
 * </ul>
 *
 * <p>Cloud locations are not checked: the server reaches them only with credentials vended for an
 * external location or configured for the bucket.
 */
public final class LocalStorageLocationValidator {

  private LocalStorageLocationValidator() {}

  /**
   * Validates the location of an external securable when it is local. A cloud location is not
   * checked.
   *
   * @throws BaseException with {@code INVALID_ARGUMENT} if the location has an escape it does not
   *     need, or {@code PERMISSION_DENIED} if the server may not write there
   */
  public static void validateLocalLocation(
      NormalizedURL location,
      ServerProperties serverProperties,
      ExternalLocationUtils externalLocationUtils) {
    URI uri = location.toUri();
    if (UriScheme.fromURI(uri) != UriScheme.FILE) {
      return;
    }
    if (!isCanonical(uri)) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Local location must not contain unneeded escapes: " + location);
    }
    // Path comparison is per segment, so /data/root-sibling is not under /data/root.
    Path path = localPath(uri);
    if (isUnderExternalLocation(location, path, externalLocationUtils)
        || isUnderLocalRoot(path, serverProperties)) {
      return;
    }
    throw new BaseException(
        ErrorCode.PERMISSION_DENIED,
        String.format(
            "Local location '%s' must be under an external location or under a root listed in"
                + " '%s'.",
            location, ServerProperties.Property.EXTERNAL_LOCAL_ROOTS.getKey()));
  }

  /**
   * Whether the URI has the one spelling {@link Path#toUri()} gives its path, such as {@code
   * tableA} and not {@code table%41}. The overlap, managed-storage, and external-location checks
   * compare locations as strings, while the server opens the decoded path, so each local directory
   * must have one accepted spelling.
   */
  private static boolean isCanonical(URI uri) {
    // Locations and stored URLs are already normalized, so compare the strings.
    return NormalizedURL.from(localPath(uri).toUri()).toString().equals(uri.toString());
  }

  private static Path localPath(URI uri) {
    return Paths.get(uri.getPath()).normalize();
  }

  /**
   * Whether an external location covers the path, comparing decoded paths. Authorization matched
   * external locations by their URL string, so it never matched one stored with an unneeded escape
   * and did not check the caller's privilege on it: a location under such an external location is
   * rejected.
   */
  private static boolean isUnderExternalLocation(
      NormalizedURL location, Path path, ExternalLocationUtils externalLocationUtils) {
    boolean covered = false;
    for (String url : externalLocationUtils.listLocalExternalLocationUrls()) {
      URI externalLocation = URI.create(url);
      if (path.startsWith(localPath(externalLocation))) {
        if (!isCanonical(externalLocation)) {
          throw new BaseException(
              ErrorCode.PERMISSION_DENIED,
              String.format(
                  "Local location '%s' is under external location '%s', whose URL has unneeded"
                      + " escapes.",
                  location, url));
        }
        covered = true;
      }
    }
    return covered;
  }

  private static boolean isUnderLocalRoot(Path path, ServerProperties serverProperties) {
    return serverProperties.getExternalLocalRoots().stream()
        .anyMatch(root -> path.startsWith(root) && !path.equals(root));
  }
}
