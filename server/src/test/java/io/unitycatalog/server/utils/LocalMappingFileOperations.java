package io.unitycatalog.server.utils;

import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.service.credential.CredentialContext;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsPrefixOperations;

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
    return mappingFileIO(path, privileges);
  }

  @Override
  public SupportsPrefixOperations getCleanupFileIO(
      NormalizedURL path, CooperativeDeadline deadline) {
    UriScheme scheme = UriScheme.fromURI(path.toUri());
    if (scheme == UriScheme.FILE || scheme == UriScheme.NULL) {
      return delegate.getCleanupFileIO(path, deadline);
    }
    // The mapping FileIO also serves prefix operations, so cleanup runs on the mapped local dir.
    // Cloud cleanup vends read/write credentials in production, so vend and validate the same here.
    // The deadline drives cancellation only for real cloud IO, which the local mapping does not do.
    return mappingFileIO(path, CredentialContext.READ_WRITE);
  }

  /**
   * Builds the cloud-to-local mapping FileIO for a cloud {@code path}: vends the real config (so
   * the fake validates the credentials UC produced) and maps IO to the registered local dir. The
   * privileges are passed through so a read-only FileIO refuses writes.
   */
  private LocalMappingFileIO mappingFileIO(
      NormalizedURL path, Set<CredentialContext.Privilege> privileges) {
    if (cloudPrefix == null) {
      throw new IllegalStateException("No local mapping registered for cloud location: " + path);
    }
    return new LocalMappingFileIO(
        cloudPrefix,
        localDir,
        delegate.getFileIOConfig(path, privileges),
        expectedCredentials,
        privileges);
  }

  @Override
  public Map<String, String> getFileIOConfig(
      NormalizedURL path,
      Set<CredentialContext.Privilege> privileges,
      Optional<String> credentialsEndpoint) {
    return delegate.getFileIOConfig(path, privileges, credentialsEndpoint);
  }
}
