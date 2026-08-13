package io.unitycatalog.hadoop.internal.fs;

import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.CredentialUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.id.FileSystemCredId;
import io.unitycatalog.hadoop.internal.util.BoundedKeyedCache;
import io.unitycatalog.hadoop.internal.util.CloseableUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
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
    List<String> credPrefixes = getCredPrefixes(conf);
    if (credPrefixes.size() <= 1) {
      // No selection is necessary when there is only one credential (there is only one choice).
      String prefix = credPrefixes.isEmpty() ? null : credPrefixes.get(0);
      FileSystemCredId key = FileSystemCredId.create(conf, uri, prefix);
      this.fs = CACHE.getOrLoad(key, () -> copyConfAndCreateNewFileSystem(uri, conf, prefix));
    } else {
      // When there are multiple credentials (credPrefixes > 1), select the (longest) prefix that
      // covers the requested URI.
      FileSystemCredId key = getFileSystemCredId(uri, conf, credPrefixes);
      this.fs = CACHE.getOrLoad(key, () -> copyConfAndCreateNewFileSystem(uri, conf, key.prefix()));
    }
  }

  private static List<String> getCredPrefixes(Configuration conf) {
    String[] encodedCredPrefixes =
        conf.getStrings(UCHadoopConfConstants.UC_CREDENTIAL_PREFIXES_KEY);
    if (encodedCredPrefixes == null || encodedCredPrefixes.length == 0) {
      return Collections.emptyList();
    }
    return CredentialUtil.decodeCredPrefixes(encodedCredPrefixes);
  }

  private static FileSystemCredId getFileSystemCredId(
      URI uri, Configuration conf, List<String> credPrefixes) {
    int selectedIndex = CredentialUtil.longestCoveringIndex(uri.toString(), credPrefixes);
    Preconditions.checkArgument(
        selectedIndex >= 0, "No credential covers storage location %s", uri);
    String selectedPrefix = credPrefixes.get(selectedIndex);
    return FileSystemCredId.create(conf, uri, selectedPrefix);
  }

  /**
   * Restores {@code key} from its {@code key.original} side-channel saved by {@link
   * io.unitycatalog.hadoop.internal.CredPropsUtil}, falling back to {@code defaultImpl} when the
   * side-channel is absent.
   */
  private static void restoreImpl(Configuration fsConf, String key, String defaultImpl) {
    fsConf.set(key, fsConf.get(key + ".original", defaultImpl));
  }

  private static FileSystem copyConfAndCreateNewFileSystem(
      URI uri, Configuration conf, String prefix) {
    Configuration fsConf = new Configuration(conf);
    if (prefix != null) {
      fsConf.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, prefix);
    }
    return newFileSystem(uri, fsConf);
  }

  // Creates a files system from the provided configuration. Note: modifies the conf in-place.
  private static FileSystem newFileSystem(URI uri, Configuration fsConf) {
    try {
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
