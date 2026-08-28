package io.unitycatalog.server.utils;

import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.service.credential.CredentialContext;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.io.FileIO;

/**
 * Decorating {@link FileOperations} for tests: serves cloud locations under a single registered
 * prefix from a local directory (via {@link LocalMappingFileIO}) while delegating local paths and
 * all credential vending to the wrapped real instance. Register the mapping with {@link
 * #mapLocation} after the server starts; a cloud access outside the prefix fails. Wire it via
 * {@code UnityCatalogServer.Builder#fileOperations} / {@code
 * BaseServerTest#decorateFileOperations}.
 */
public class LocalMappingFileOperations implements FileOperations {

  private final FileOperations delegate;
  private final Map<String, String> expectedCredentials;
  private NormalizedURL cloudPrefix;
  private Path localDir;

  /**
   * @param expectedCredentials config-key → expected value that UC's test credential vendor should
   *     have produced; passed through to {@link LocalMappingFileIO} for exact-match validation.
   */
  public LocalMappingFileOperations(
      FileOperations delegate, Map<String, String> expectedCredentials) {
    this.delegate = delegate;
    this.expectedCredentials = expectedCredentials;
  }

  /**
   * Serves cloud locations under {@code cloudPrefix} from {@code localDir}. May be called only
   * once: a single test maps a single cloud root.
   */
  public void mapLocation(NormalizedURL cloudPrefix, Path localDir) {
    if (this.cloudPrefix != null) {
      throw new IllegalStateException(
          "A location mapping is already registered: " + this.cloudPrefix);
    }
    this.cloudPrefix = cloudPrefix;
    this.localDir = localDir;
  }

  /** The local path a mapped cloud location resolves to (for test assertions). */
  public Path localPathOf(NormalizedURL cloudLocation) {
    return LocalMappingFileIO.toLocalPath(cloudLocation.toString(), cloudPrefix, localDir);
  }

  @Override
  public FileIO getFileIO(NormalizedURL path, Set<CredentialContext.Privilege> privileges) {
    UriScheme scheme = UriScheme.fromURI(path.toUri());
    if (scheme == UriScheme.FILE || scheme == UriScheme.NULL) {
      return delegate.getFileIO(path, privileges);
    }
    if (cloudPrefix == null) {
      throw new IllegalStateException("No local mapping registered for cloud location: " + path);
    }
    // Vend real credentials (as production getFileIO does) and hand them to the fake, which
    // validates them before mapping IO to the registered local dir. The privileges are passed
    // through so a read-only FileIO refuses writes, as read-only cloud credentials would.
    return new LocalMappingFileIO(
        cloudPrefix,
        localDir,
        delegate.getFileIOConfig(path, privileges),
        expectedCredentials,
        privileges);
  }

  @Override
  public Map<String, String> getFileIOConfig(
      NormalizedURL path, Set<CredentialContext.Privilege> privileges) {
    return delegate.getFileIOConfig(path, privileges);
  }
}
