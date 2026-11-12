package io.unitycatalog.hadoop.internal.fs;

import static io.unitycatalog.hadoop.internal.id.CredIdTest.EMPTY_CRED_CONTEXT_ID;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.hadoop.internal.CredentialUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.util.MapIdGenerator;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies the caching behaviour of {@link CredScopedFileSystem}: same credential scope reuses the
 * same delegate, different scopes get independent instances, closing a wrapper leaves the shared
 * delegate open, and an evicted delegate is closed once no open wrapper uses it.
 *
 * <p>Uses {@code file://} URIs with the local filesystem so no cloud SDK is required.
 */
class CredScopedFileSystemCacheTest {

  private static final Path PATH = new Path("file:///tmp/any");
  private static final Duration GC_TIMEOUT = Duration.ofSeconds(10);

  private static final String CONTEXT_A =
      MapIdGenerator.generateId(Map.of("type", "static", "token", "tenant-a"));
  private static final String CONTEXT_B =
      MapIdGenerator.generateId(Map.of("type", "static", "token", "tenant-b"));

  @AfterEach
  void clearCache() {
    CredScopedFileSystem.clearCacheForTesting();
  }

  private static CredScopedFileSystem init(URI uri, Configuration conf) throws Exception {
    CredScopedFileSystem fs = new CredScopedFileSystem();
    fs.initialize(uri, conf);
    return fs;
  }

  private static Configuration tableConf(String tableId, String op) {
    return tableConf(tableId, op, EMPTY_CRED_CONTEXT_ID);
  }

  private static Configuration tableConf(String tableId, String op, String credContextId) {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY, credContextId);
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_TABLE_ID_KEY, tableId);
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, op);
    conf.set("fs.file.impl.disable.cache", "true");
    return conf;
  }

  private static void setCredPrefixes(Configuration conf, String... prefixes) {
    conf.setStrings(
        UCHadoopConfConstants.UC_CREDENTIAL_PREFIXES_KEY,
        CredentialUtil.encodeCredPrefixes(List.of(prefixes)));
  }

  @Test
  void sameScopeReusesSameDelegate() throws Exception {
    URI uri = new URI("file:///tmp");
    Configuration conf = tableConf("tid-1", "READ");

    CredScopedFileSystem fs1 = init(uri, conf);
    CredScopedFileSystem fs2 = init(uri, conf);

    assertThat(fs1.getRawFileSystem()).isSameAs(fs2.getRawFileSystem());
  }

  @Test
  void noCredentialRequestUsesDefaultFilesystemWithoutUcProvider() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("fs.file.impl", RawLocalFileSystem.class.getName());
    conf.setBoolean("fs.file.impl.disable.cache", true);

    CredScopedFileSystem fsA = init(new URI("file:///tmp/a"), conf);
    CredScopedFileSystem fsB = init(new URI("file:///tmp/b"), conf);

    assertThat(fsA.getRawFileSystem()).isInstanceOf(RawLocalFileSystem.class);
    assertThat(fsA.getRawFileSystem()).isSameAs(fsB.getRawFileSystem());
    assertThat(fsA.getRawFileSystem().getConf().get(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY))
        .isNull();
    assertThat(fsA.getRawFileSystem().getConf().get(UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER))
        .isNull();
  }

  @Test
  void differentScopeGetsDifferentDelegate() throws Exception {
    URI uri = new URI("file:///tmp");

    CredScopedFileSystem fsRead = init(uri, tableConf("tid-1", "READ"));
    CredScopedFileSystem fsWrite = init(uri, tableConf("tid-1", "WRITE"));

    assertThat(fsRead.getRawFileSystem()).isNotSameAs(fsWrite.getRawFileSystem());
  }

  @Test
  void differentScopeSamePrefixGetsDifferentDelegate() throws Exception {
    URI uri = new URI("file:///tmp");
    Configuration confRead = tableConf("tid-1", "READ");
    setCredPrefixes(confRead, "file:///tmp/a");
    Configuration confWrite = tableConf("tid-1", "WRITE");
    setCredPrefixes(confWrite, "file:///tmp/a");

    CredScopedFileSystem fsRead = init(uri, confRead);
    CredScopedFileSystem fsWrite = init(uri, confWrite);

    assertThat(fsRead.getRawFileSystem()).isNotSameAs(fsWrite.getRawFileSystem());
  }

  @Test
  void sameTableDifferentCredContextGetsDifferentDelegate() throws Exception {
    URI uri = new URI("file:///tmp");

    CredScopedFileSystem fsTenantA = init(uri, tableConf("tid-1", "READ", CONTEXT_A));
    CredScopedFileSystem fsTenantB = init(uri, tableConf("tid-1", "READ", CONTEXT_B));

    assertThat(fsTenantA.getRawFileSystem()).isNotSameAs(fsTenantB.getRawFileSystem());
  }

  @Test
  void sameScopeDifferentPrefixGetsDifferentDelegate() throws Exception {
    URI uri = new URI("file:///tmp");
    Configuration confA = tableConf("tid-1", "READ");
    setCredPrefixes(confA, "file:///tmp/a");
    Configuration confB = tableConf("tid-1", "READ");
    setCredPrefixes(confB, "file:///tmp/b");

    CredScopedFileSystem fsA = init(uri, confA);
    CredScopedFileSystem fsB = init(uri, confB);

    assertThat(fsA.getRawFileSystem()).isNotSameAs(fsB.getRawFileSystem());
  }

  @Test
  void sameScopeSamePrefixReusesSameDelegate() throws Exception {
    URI uri = new URI("file:///tmp");
    Configuration conf = tableConf("tid-1", "READ");
    setCredPrefixes(conf, "file:///tmp/a");

    CredScopedFileSystem fs1 = init(uri, conf);
    CredScopedFileSystem fs2 = init(uri, conf);

    assertThat(fs1.getRawFileSystem()).isSameAs(fs2.getRawFileSystem());
  }

  @Test
  void differentlyFormattedPrefixesGetDifferentDelegates() throws Exception {
    URI uri = new URI("file:///tmp");
    Configuration confA = tableConf("tid-1", "READ");
    setCredPrefixes(confA, "file:///tmp/a");
    Configuration confB = tableConf("tid-1", "READ");
    setCredPrefixes(confB, "file:///tmp/a/");

    CredScopedFileSystem fsA = init(uri, confA);
    CredScopedFileSystem fsB = init(uri, confB);

    assertThat(fsA.getRawFileSystem()).isNotSameAs(fsB.getRawFileSystem());
  }

  @Test
  void multiCredSelectionDeterminesDelegateIdentity() throws Exception {
    Configuration conf = tableConf("tid-1", "READ");
    conf.setStrings(
        UCHadoopConfConstants.UC_CREDENTIAL_PREFIXES_KEY,
        CredentialUtil.encodeCredPrefixes(List.of("file:///tmp/a", "file:///tmp/b")));

    // URIs covered by the same credential resolve to one delegate; different ones do not.
    CredScopedFileSystem fsA1 = init(new URI("file:///tmp/a/one"), conf);
    CredScopedFileSystem fsA2 = init(new URI("file:///tmp/a/two"), conf);
    CredScopedFileSystem fsB = init(new URI("file:///tmp/b/one"), conf);

    assertThat(fsA1.getRawFileSystem()).isSameAs(fsA2.getRawFileSystem());
    assertThat(fsA1.getRawFileSystem()).isNotSameAs(fsB.getRawFileSystem());
  }

  @Test
  void evictedDelegateWithoutOpenWrappersIsClosed() throws Exception {
    CredScopedFileSystem fs = init(URI.create("file:///tmp"), trackedConf("tid-evict"));
    CloseTrackingFileSystem delegate = (CloseTrackingFileSystem) fs.getRawFileSystem();
    fs.close();

    CredScopedFileSystem.clearCacheForTesting();

    assertThat(delegate.closed).isTrue();
  }

  @Test
  void closingOneWrapperKeepsOtherWrappersOfSameScopeWorking() throws Exception {
    URI uri = URI.create("file:///tmp");
    Configuration conf = trackedConf("tid-1");
    CredScopedFileSystem first = init(uri, conf);
    CredScopedFileSystem second = init(uri, conf);

    first.close();

    assertThat(second.getFileStatus(PATH)).isNotNull();
  }

  @Test
  void wrapperCreatedAfterAnotherWrapperClosedWorks() throws Exception {
    URI uri = URI.create("file:///tmp");
    Configuration conf = trackedConf("tid-1");
    init(uri, conf).close();

    CredScopedFileSystem fresh = init(uri, conf);

    assertThat(fresh.getFileStatus(PATH)).isNotNull();
  }

  @Test
  void evictionDoesNotBreakAnOpenWrapper() throws Exception {
    URI uri = URI.create("file:///tmp");
    CredScopedFileSystem held = init(uri, trackedConf("tid-held"));

    int maxSize = CredScopedFileSystem.CACHE.maxSize();
    for (int i = 0; i < maxSize; i++) {
      init(uri, trackedConf("tid-other-" + i));
    }

    assertThat(CredScopedFileSystem.CACHE.size()).isEqualTo(maxSize);
    assertThat(held.getFileStatus(PATH)).isNotNull();
  }

  @Test
  void evictedDelegateIsClosedWhenItsLastWrapperCloses() throws Exception {
    URI uri = URI.create("file:///tmp");
    Configuration conf = trackedConf("tid-1");
    CredScopedFileSystem first = init(uri, conf);
    CredScopedFileSystem second = init(uri, conf);
    CloseTrackingFileSystem delegate = (CloseTrackingFileSystem) first.getRawFileSystem();

    CredScopedFileSystem.clearCacheForTesting();
    first.close();
    assertThat(delegate.closed).isFalse();

    second.close();
    assertThat(delegate.closed).isTrue();
  }

  @Test
  void closingAWrapperTwiceReleasesItOnlyOnce() throws Exception {
    URI uri = URI.create("file:///tmp");
    Configuration conf = trackedConf("tid-1");
    CredScopedFileSystem first = init(uri, conf);
    CredScopedFileSystem second = init(uri, conf);
    CloseTrackingFileSystem delegate = (CloseTrackingFileSystem) first.getRawFileSystem();

    CredScopedFileSystem.clearCacheForTesting();
    first.close();
    first.close();

    assertThat(delegate.closed).isFalse();
    assertThat(second.getFileStatus(PATH)).isNotNull();
  }

  @Test
  void closingAWrapperFromNewInstanceRemovesItFromHadoopCache() throws Exception {
    Configuration conf = trackedConf("tid-1");
    conf.set("fs.s3a.impl", CredScopedFileSystem.class.getName());
    conf.set("fs.s3a.impl.original", CloseTrackingFileSystem.class.getName());
    int sizeBefore = hadoopCacheSize();

    FileSystem fs = FileSystem.newInstance(URI.create("s3a://bucket/"), conf);
    assertThat(fs).isInstanceOf(CredScopedFileSystem.class);
    assertThat(hadoopCacheSize()).isEqualTo(sizeBefore + 1);

    fs.close();
    assertThat(hadoopCacheSize()).isEqualTo(sizeBefore);
  }

  @Test
  void evictedDelegateIsClosedOnceItsNeverClosedWrapperIsGarbageCollected() throws Exception {
    AtomicReference<CredScopedFileSystem> onlyWrapper =
        new AtomicReference<>(init(URI.create("file:///tmp"), trackedConf("tid-gc")));
    CloseTrackingFileSystem delegate =
        (CloseTrackingFileSystem) onlyWrapper.get().getRawFileSystem();
    CredScopedFileSystem.clearCacheForTesting();
    assertThat(delegate.closed).isFalse();
    onlyWrapper.set(null);

    long deadline = System.nanoTime() + GC_TIMEOUT.toNanos();
    while (!delegate.closed && System.nanoTime() < deadline) {
      System.gc();
      Thread.sleep(50);
    }

    assertThat(delegate.closed).isTrue();
  }

  private static int hadoopCacheSize() throws Exception {
    Method cacheSize = FileSystem.class.getDeclaredMethod("cacheSize");
    cacheSize.setAccessible(true);
    return (int) cacheSize.invoke(null);
  }

  private static Configuration trackedConf(String tableId) {
    Configuration conf = tableConf(tableId, "READ");
    conf.set("fs.file.impl", CloseTrackingFileSystem.class.getName());
    return conf;
  }

  /** Local filesystem stand-in that, like cloud filesystems, rejects calls once closed. */
  public static class CloseTrackingFileSystem extends RawLocalFileSystem {
    volatile boolean closed;

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      if (closed) {
        throw new IOException("Filesystem closed");
      }
      return new FileStatus(0, true, 1, 0, 0, path);
    }

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }
  }
}
