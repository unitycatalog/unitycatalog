package io.unitycatalog.hadoop.internal.fs;

import static io.unitycatalog.hadoop.internal.id.CredIdTest.EMPTY_CRED_CONTEXT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.id.CredId;
import io.unitycatalog.hadoop.internal.id.DelegateFileSystemId;
import io.unitycatalog.hadoop.internal.id.TableCredId;
import io.unitycatalog.hadoop.internal.util.MapIdGenerator;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies the caching behaviour of {@link CredScopedFileSystem}: same credential scope reuses the
 * same delegate, different scopes get independent instances, and evicted entries are closed.
 *
 * <p>Uses {@code file://} URIs with the local filesystem so no cloud SDK is required.
 */
class CredScopedFileSystemCacheTest {

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

  /** Encodes a scoped credential's location and a placeholder credential key at {@code index}. */
  private static void setScopedLocation(Configuration conf, int index, String location) {
    String namespace = UCHadoopConfConstants.UC_SCOPED_CRED_PREFIX + index + ".";
    conf.set(namespace + UCHadoopConfConstants.UC_CREDENTIAL_LOCATION_KEY, location);
    conf.set(namespace + "fs.unitycatalog.test.credential", "credential-" + index);
  }

  @Test
  void sameScopeReusesSameDelegate() throws Exception {
    URI uri = new URI("file:///tmp");
    Configuration conf = tableConf("tid-1", "READ");

    CredScopedFileSystem fs1 = init(uri, conf);
    CredScopedFileSystem fs2 = init(uri, conf);

    assertThat(fs1.getDelegate()).isSameAs(fs2.getDelegate());
  }

  @Test
  void differentScopeGetsDifferentDelegate() throws Exception {
    URI uri = new URI("file:///tmp");

    CredScopedFileSystem fsRead = init(uri, tableConf("tid-1", "READ"));
    CredScopedFileSystem fsWrite = init(uri, tableConf("tid-1", "WRITE"));

    assertThat(fsRead.getDelegate()).isNotSameAs(fsWrite.getDelegate());
  }

  @Test
  void sameTableDifferentCredContextGetsDifferentDelegate() throws Exception {
    URI uri = new URI("file:///tmp");

    CredScopedFileSystem fsTenantA = init(uri, tableConf("tid-1", "READ", CONTEXT_A));
    CredScopedFileSystem fsTenantB = init(uri, tableConf("tid-1", "READ", CONTEXT_B));

    assertThat(fsTenantA.getDelegate()).isNotSameAs(fsTenantB.getDelegate());
  }

  @Test
  void sameScopeDifferentLocationGetsDifferentDelegate() throws Exception {
    URI uri = new URI("file:///tmp");
    Configuration confA = tableConf("tid-1", "READ");
    confA.set(UCHadoopConfConstants.UC_CREDENTIAL_LOCATION_KEY, "file:///tmp/a");
    Configuration confB = tableConf("tid-1", "READ");
    confB.set(UCHadoopConfConstants.UC_CREDENTIAL_LOCATION_KEY, "file:///tmp/b");

    CredScopedFileSystem fsA = init(uri, confA);
    CredScopedFileSystem fsB = init(uri, confB);

    assertThat(fsA.getDelegate()).isNotSameAs(fsB.getDelegate());
  }

  @Test
  void sameScopeSameLocationReusesSameDelegate() throws Exception {
    URI uri = new URI("file:///tmp");
    Configuration conf = tableConf("tid-1", "READ");
    conf.set(UCHadoopConfConstants.UC_CREDENTIAL_LOCATION_KEY, "file:///tmp/a");

    CredScopedFileSystem fs1 = init(uri, conf);
    CredScopedFileSystem fs2 = init(uri, conf);

    assertThat(fs1.getDelegate()).isSameAs(fs2.getDelegate());
  }

  @Test
  void namespacedSelectedPrefixDeterminesDelegateIdentity() throws Exception {
    Configuration conf = tableConf("tid-1", "READ");
    conf.setInt(UCHadoopConfConstants.UC_SCOPED_CRED_COUNT_KEY, 2);
    setScopedLocation(conf, 0, "file:///tmp/a");
    setScopedLocation(conf, 1, "file:///tmp/b");

    // URIs covered by the same credential resolve to one delegate; different ones do not.
    CredScopedFileSystem fsA1 = init(new URI("file:///tmp/a/one"), conf);
    CredScopedFileSystem fsA2 = init(new URI("file:///tmp/a/two"), conf);
    CredScopedFileSystem fsB = init(new URI("file:///tmp/b/one"), conf);

    assertThat(fsA1.getDelegate()).isSameAs(fsA2.getDelegate());
    assertThat(fsA1.getDelegate()).isNotSameAs(fsB.getDelegate());
  }

  @Test
  void evictedEntryClosesCachedDelegate() throws Exception {
    FileSystem mockFs = mock(FileSystem.class);
    CredId credId = new TableCredId(EMPTY_CRED_CONTEXT_ID, "tid-evict", "READ");
    DelegateFileSystemId key = DelegateFileSystemId.of(credId, null);
    CredScopedFileSystem.CACHE.put(key, mockFs);

    CredScopedFileSystem.clearCacheForTesting();

    verify(mockFs).close();
  }
}
