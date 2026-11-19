package io.unitycatalog.hadoop.internal.fs;

import io.unitycatalog.client.internal.BoundedKeyedCache;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.CredentialUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.id.FileSystemCredId;
import io.unitycatalog.hadoop.internal.util.CloseableUtils;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
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
 *       CredScopedFileSystem} for each file access. Because {@code CredScopedFileSystem} is a thin
 *       wrapper that only holds a reference to a shared delegate, this is cheap.
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
 *
 * <h2>Delegate lifetime</h2>
 *
 * <p>The cache owns each delegate, and every wrapper of the same scope shares it. Each wrapper
 * therefore wraps its own {@code Lease} on the delegate rather than the delegate itself, so closing
 * a wrapper runs the regular {@link FileSystem#close()} bookkeeping and then releases the lease,
 * even when that bookkeeping throws. A wrapper that is never closed releases its lease once the
 * lease is garbage collected. A wrapper obtained through {@link FileSystem#newInstance(URI,
 * Configuration)} is held by Hadoop's cache until it is closed, so only {@code close()} releases
 * it. A delegate is closed once it has been evicted from the cache and every lease on it has been
 * released, so it is not closed while an open wrapper of it is strongly reachable. The cache's
 * maximum size therefore bounds the cached delegates, not the open ones: an evicted delegate stays
 * open while a wrapper still uses it, and a cached delegate stays open after its last wrapper
 * closes until it is evicted.
 *
 * <p>Nothing else keeps a lease alive. A call still running on a wrapper that the caller no longer
 * references, as in {@code path.getFileSystem(conf).listStatus(path)}, and the streams, iterators
 * and builders that a wrapper returns can find an evicted delegate closed under them once the
 * wrapper has been collected. See <a
 * href="https://github.com/unitycatalog/unitycatalog/issues/2191">issue #2191</a>.
 *
 * <p>These guarantees hold for delegates that are not shared outside this cache. For the {@code
 * s3}, {@code s3a}, {@code gs}, {@code abfs} and {@code abfss} schemes the delegate is created with
 * Hadoop's own cache disabled. For any other scheme, including {@code wasb}, {@link
 * FileSystem#get(URI, Configuration)} can return a JVM-wide instance from Hadoop's cache, which
 * other scopes and callers may share, and evicting a scope closes it regardless.
 */
public class CredScopedFileSystem extends FilterFileSystem {

  private static final String CRED_SCOPED_FS_CACHE_MAX_SIZE =
      "unitycatalog.credScopedFs.cache.maxSize";
  private static final int CRED_SCOPED_FS_CACHE_MAX_SIZE_DEFAULT = 100;

  /** The effective maximum size of {@link #CACHE}. Visible for testing. */
  static final int CACHE_MAX_SIZE =
      Integer.getInteger(CRED_SCOPED_FS_CACHE_MAX_SIZE, CRED_SCOPED_FS_CACHE_MAX_SIZE_DEFAULT);

  private static final Cleaner CLEANER = Cleaner.create();

  /**
   * LRU cache of real {@link FileSystem} instances keyed by {@link FileSystemCredId}. Evicted
   * entries are closed as described in the class javadoc, to release connection pools and SDK
   * thread pools (e.g. AWS sdk-ScheduledExecutor threads). The maximum size can be tuned via the
   * system property {@code unitycatalog.credScopedFs.cache.maxSize}.
   *
   * <p>Visible for testing.
   */
  static final BoundedKeyedCache<FileSystemCredId, SharedFileSystem> CACHE =
      new BoundedKeyedCache<>(CACHE_MAX_SIZE, SharedFileSystem::evict);

  /** Visible for testing only. Clears the static cache, closing delegates no open wrapper uses. */
  static void clearCacheForTesting() {
    CACHE.clear();
  }

  private Lease lease;

  /**
   * Returns the shared delegate, or {@code null} before {@link #initialize(URI, Configuration)}.
   * The delegate is not leased to the caller, so callers must not close it.
   */
  @Override
  public FileSystem getRawFileSystem() {
    return lease == null ? null : lease.getRawFileSystem();
  }

  @Override
  public FileSystem[] getChildFileSystems() {
    return new FileSystem[] {getRawFileSystem()};
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      if (lease != null) {
        lease.close();
      }
    }
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    List<String> credPrefixes = getCredPrefixes(conf);
    if (credPrefixes.size() <= 1) {
      // No selection is necessary when there is only one credential (there is only one choice).
      String prefix = credPrefixes.isEmpty() ? null : credPrefixes.get(0);
      FileSystemCredId key = FileSystemCredId.create(conf, uri, prefix);
      acquire(key, () -> copyConfAndCreateNewFileSystem(uri, conf, prefix));
    } else {
      // When there are multiple credentials (credPrefixes > 1), select the (longest) prefix that
      // covers the requested URI.
      FileSystemCredId key = getFileSystemCredId(uri, conf, credPrefixes);
      acquire(key, () -> copyConfAndCreateNewFileSystem(uri, conf, key.prefix()));
    }
  }

  private void acquire(FileSystemCredId key, Supplier<FileSystem> loader) {
    // A delegate loaded here is published with this wrapper's lease already taken, so a concurrent
    // eviction cannot close it before it is used. A cached entry evicted between the lookup and
    // acquire() is already gone from the cache, so the retry leases whichever delegate is cached
    // next, either one it loads itself or one another thread has loaded.
    SharedFileSystem[] loaded = new SharedFileSystem[1];
    SharedFileSystem shared;
    do {
      try {
        shared = CACHE.getOrLoad(key, () -> loaded[0] = new SharedFileSystem(loader.get()));
      } catch (RuntimeException | Error e) {
        // An eviction listener can throw after the loaded entry was published with this wrapper's
        // lease counted, so give the lease back or the entry is never closed once evicted.
        if (loaded[0] != null) {
          loaded[0].release();
        }
        throw e;
      }
    } while (shared != loaded[0] && !shared.acquire());
    this.lease = new Lease(shared);
    this.fs = lease;
  }

  /** One wrapper's lease on a shared delegate. Closing it releases the lease, not the delegate. */
  // TODO(#2191): fence forwarded calls and lease returned streams, iterators and builders.
  private static final class Lease extends FilterFileSystem {
    private final Cleaner.Cleanable release;

    private Lease(SharedFileSystem shared) {
      super(shared.fs);
      this.release = CLEANER.register(this, shared::release);
    }

    @Override
    public void close() {
      release.clean();
    }
  }

  /** A cached delegate plus the number of open wrappers that use it. */
  static final class SharedFileSystem {
    private final FileSystem fs;
    private int openWrappers;
    private boolean evicted;

    /** Creates an entry that already counts the lease of the wrapper that loads it. */
    private SharedFileSystem(FileSystem fs) {
      this.fs = fs;
      this.openWrappers = 1;
    }

    private synchronized boolean acquire() {
      if (evicted) {
        return false;
      }
      openWrappers++;
      return true;
    }

    private synchronized void release() {
      openWrappers--;
      closeIfUnused();
    }

    private synchronized void evict() {
      evicted = true;
      closeIfUnused();
    }

    // TODO(#2191): close outside the monitor, outside the cache's key lock on eviction, and off the
    // shared Cleaner thread, which a slow close otherwise stalls for every other release.
    private void closeIfUnused() {
      if (evicted && openWrappers == 0) {
        CloseableUtils.closeQuietly(fs);
      }
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
