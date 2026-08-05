package io.unitycatalog.hadoop.internal.fs;

import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.CredentialUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.id.FileSystemCredId;
import io.unitycatalog.hadoop.internal.util.BoundedKeyedCache;
import io.unitycatalog.hadoop.internal.util.CloseableUtils;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;

/**
 * A Hadoop {@link FileSystem} wrapper that enables multiple credential scopes to coexist within a
 * single Spark session.
 *
 * <h2>Problem</h2>
 *
 * <p>Hadoop's native {@link FileSystem} is designed around a single credential per scheme: its
 * internal cache maps {@code (scheme, authority)} to a shared {@link FileSystem} instance, so all
 * operations on the same URI share the same credential. Unity Catalog, however, vends per-table and
 * per-path temporary credentials, meaning that two different tables backed by the same S3 bucket
 * may require entirely different AWS session tokens at the same time. Simply disabling Hadoop's
 * cache (e.g. {@code fs.s3a.impl.disable.cache=true}) would work functionally but creates a new
 * underlying {@link FileSystem} instance for every file access, quickly exhausting native resources
 * such as S3A connection pools (see <a
 * href="https://github.com/unitycatalog/unitycatalog/issues/1378">issue #1378</a>).
 *
 * <h2>Solution</h2>
 *
 * <p>This class introduces a two-level caching strategy:
 *
 * <ol>
 *   <li><b>Hadoop cache disabled for {@code CredScopedFileSystem} itself.</b> {@link
 *       io.unitycatalog.hadoop.internal.CredPropsUtil} sets {@code
 *       fs.<scheme>.impl.disable.cache=true} so that Hadoop always instantiates a fresh {@code
 *       CredScopedFileSystem} for each file access. Because {@code CredScopedFileSystem} is a thin,
 *       stateless wrapper, this is cheap.
 *   <li><b>Global credential-scoped cache for the real delegate.</b> {@code CredScopedFileSystem}
 *       maintains a static {@link #CACHE} keyed by {@link FileSystemCredId}. On each {@link
 *       #initialize(URI, Configuration)} call the key is derived from the Hadoop {@link
 *       Configuration} injected by {@link io.unitycatalog.hadoop.internal.CredPropsUtil}, and the
 *       corresponding real {@link FileSystem} (e.g. {@code S3AFileSystem}) is looked up or created.
 *       Requests that share the same key reuse the same underlying connection pool, while other
 *       requests receive an isolated instance.
 * </ol>
 *
 * <p>All public {@link FileSystem} operations are delegated to the credential-scoped instance via
 * {@link FilterFileSystem}, so callers see a fully functional filesystem regardless of which
 * underlying implementation backs it.
 */
public class CredScopedFileSystem extends FilterFileSystem {

  private static final String CRED_SCOPED_FS_CACHE_MAX_SIZE =
      "unitycatalog.credScopedFs.cache.maxSize";
  private static final int CRED_SCOPED_FS_CACHE_MAX_SIZE_DEFAULT = 100;

  // Maximum number of namespaced credentials read from the configuration to prevent OOM.
  private static final String MAX_MULTI_CRED_COUNT_PROPERTY = "unitycatalog.multi.cred.maxCount";
  private static final int MAX_MULTI_CRED_COUNT_DEFAULT = 10;
  private static final int MAX_MULTI_CRED_COUNT =
      Integer.getInteger(MAX_MULTI_CRED_COUNT_PROPERTY, MAX_MULTI_CRED_COUNT_DEFAULT);

  /**
   * LRU cache of real {@link FileSystem} instances keyed by {@link FileSystemCredId}. Evicted
   * entries are closed to release connection pools and SDK thread pools (e.g. AWS
   * sdk-ScheduledExecutor threads). The cache is bounded to prevent unbounded growth when many
   * distinct credential scopes are accessed in a long-running session. The maximum size can be
   * tuned via the system property {@code unitycatalog.credScopedFs.cache.maxSize}.
   */
  /** Visible for testing. */
  static final BoundedKeyedCache<FileSystemCredId, FileSystem> CACHE;

  static {
    int maxSize =
        Integer.getInteger(CRED_SCOPED_FS_CACHE_MAX_SIZE, CRED_SCOPED_FS_CACHE_MAX_SIZE_DEFAULT);
    CACHE = new BoundedKeyedCache<>(maxSize, CloseableUtils::closeQuietly);
  }

  /** Visible for testing only. Clears the static cache and closes all cached delegates. */
  static void clearCacheForTesting() {
    CACHE.clear();
  }

  /** Visible for testing only. Returns the cached delegate filesystem. */
  FileSystem getDelegate() {
    return this.fs;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    String namespace = selectNamespace(uri, conf);
    FileSystemCredId key = FileSystemCredId.create(conf, uri, namespace);
    // Deriving the namespace and key does not copy the conf, delaying expensive copy
    // operation until it is needed in newFileSystem.
    this.fs = CACHE.getOrLoad(key, () -> newFileSystem(uri, conf, namespace));
  }

  /**
   * Returns the namespace key for the credential that covers the given URI, or null if no namespace
   * is needed (when there is only one credential). When there are multiple credentials, each
   * credential's details in the Hadoop configuration are prefixed with the namespace key to avoid
   * key collisions. These namespaced keys cannot be picked up downstream; they must be extracted
   * and restored to top-level keys prior to file system initialization.
   */
  private static String selectNamespace(URI uri, Configuration conf) {
    int count = conf.getInt(UCHadoopConfConstants.UC_MULTI_CRED_COUNT_KEY, 0);
    if (count == 0) {
      return null; // Single credentials should not use a namespace. Key remains at top-level.
    }
    // Multiple credentials require a namespace and should always contain more than one credential.
    // Otherwise, this is a single credential and should not set UC_MULTI_CRED_COUNT_KEY.
    Preconditions.checkArgument(
        count > 1 && count <= MAX_MULTI_CRED_COUNT,
        "Number of credentials must be greater than 1 and at most %s: %s",
        MAX_MULTI_CRED_COUNT,
        count);

    List<String> prefixes = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      String credNamespace = CredentialUtil.hadoopConfNamespaceForIndex(i);
      String credPrefixKey = credNamespace + UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY;
      String prefix = conf.get(credPrefixKey);
      // Each credential must include the prefix it covers to be selectable.
      Preconditions.checkArgument(
          prefix != null,
          "Namespaced credential %s is missing its prefix for storage location %s",
          i,
          uri);
      prefixes.add(prefix);
    }
    int selectedIndex = CredentialUtil.longestCoveringIndex(uri.toString(), prefixes);
    Preconditions.checkArgument(
        selectedIndex >= 0, "No credential covers storage location %s", uri);
    return CredentialUtil.hadoopConfNamespaceForIndex(selectedIndex);
  }

  /**
   * Restores {@code key} from its {@code key.original} side-channel saved by {@link
   * io.unitycatalog.hadoop.internal.CredPropsUtil}, falling back to {@code defaultImpl} when the
   * side-channel is absent.
   */
  private static void restoreImpl(Configuration fsConf, String key, String defaultImpl) {
    fsConf.set(key, fsConf.get(key + ".original", defaultImpl));
  }

  private static FileSystem newFileSystem(URI uri, Configuration conf, String namespace) {
    try {
      Configuration fsConf = new Configuration(conf);
      if (namespace != null) {
        // Set the namespaced keys to top-level so downstream readers can pick them up.
        conf.getPropsWithPrefix(namespace).forEach(fsConf::set);
      }

      // S3: restore impl using the side-channel key saved by CredPropsUtil before it overrode
      // fs.<scheme>.impl with CredScopedFileSystem. Falls back to S3AFileSystem if not set.
      restoreImpl(fsConf, "fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      restoreImpl(fsConf, "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      restoreImpl(fsConf, "fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A");
      restoreImpl(fsConf, "fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A");
      fsConf.set("fs.s3.impl.disable.cache", "true");
      fsConf.set("fs.s3a.impl.disable.cache", "true");

      // GCS: restore impl using the side-channel key. Falls back to GoogleHadoopFileSystem if not
      // set (registered via the Java service loader).
      restoreImpl(fsConf, "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
      restoreImpl(
          fsConf, "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
      fsConf.set("fs.gs.impl.disable.cache", "true");

      // Azure: restore impl using the side-channel key. Falls back to AzureBlobFileSystem /
      // SecureAzureBlobFileSystem if not set (registered via the Java service loader).
      restoreImpl(fsConf, "fs.abfs.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem");
      restoreImpl(
          fsConf, "fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem");
      restoreImpl(fsConf, "fs.AbstractFileSystem.abfs.impl", "org.apache.hadoop.fs.azurebfs.Abfs");
      restoreImpl(
          fsConf, "fs.AbstractFileSystem.abfss.impl", "org.apache.hadoop.fs.azurebfs.Abfss");
      fsConf.set("fs.abfs.impl.disable.cache", "true");
      fsConf.set("fs.abfss.impl.disable.cache", "true");

      return FileSystem.get(uri, fsConf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
