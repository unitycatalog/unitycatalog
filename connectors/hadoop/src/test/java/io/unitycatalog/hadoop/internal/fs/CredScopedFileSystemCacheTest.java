package io.unitycatalog.hadoop.internal.fs;

import static io.unitycatalog.hadoop.internal.id.CredIdTest.EMPTY_CRED_CONTEXT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.unitycatalog.hadoop.internal.CredentialUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.id.FileSystemCredId;
import io.unitycatalog.hadoop.internal.util.MapIdGenerator;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
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

    assertThat(fs1.getDelegate()).isSameAs(fs2.getDelegate());
  }

  @Test
  void noCredentialRequestUsesDefaultFilesystemWithoutUcProvider() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("fs.file.impl", RawLocalFileSystem.class.getName());
    conf.setBoolean("fs.file.impl.disable.cache", true);

    CredScopedFileSystem fsA = init(new URI("file:///tmp/a"), conf);
    CredScopedFileSystem fsB = init(new URI("file:///tmp/b"), conf);

    assertThat(fsA.getDelegate()).isInstanceOf(RawLocalFileSystem.class);
    assertThat(fsA.getDelegate()).isSameAs(fsB.getDelegate());
    assertThat(fsA.getDelegate().getConf().get(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY))
        .isNull();
    assertThat(fsA.getDelegate().getConf().get(UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER))
        .isNull();
  }

  @Test
  void differentScopeGetsDifferentDelegate() throws Exception {
    URI uri = new URI("file:///tmp");

    CredScopedFileSystem fsRead = init(uri, tableConf("tid-1", "READ"));
    CredScopedFileSystem fsWrite = init(uri, tableConf("tid-1", "WRITE"));

    assertThat(fsRead.getDelegate()).isNotSameAs(fsWrite.getDelegate());
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
  void sameScopeDifferentPrefixGetsDifferentDelegate() throws Exception {
    URI uri = new URI("file:///tmp");
    Configuration confA = tableConf("tid-1", "READ");
    setCredPrefixes(confA, "file:///tmp/a");
    Configuration confB = tableConf("tid-1", "READ");
    setCredPrefixes(confB, "file:///tmp/b");

    CredScopedFileSystem fsA = init(uri, confA);
    CredScopedFileSystem fsB = init(uri, confB);

    assertThat(fsA.getDelegate()).isNotSameAs(fsB.getDelegate());
  }

  @Test
  void sameScopeSamePrefixReusesSameDelegate() throws Exception {
    URI uri = new URI("file:///tmp");
    Configuration conf = tableConf("tid-1", "READ");
    setCredPrefixes(conf, "file:///tmp/a");

    CredScopedFileSystem fs1 = init(uri, conf);
    CredScopedFileSystem fs2 = init(uri, conf);

    assertThat(fs1.getDelegate()).isSameAs(fs2.getDelegate());
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

    assertThat(fsA.getDelegate()).isNotSameAs(fsB.getDelegate());
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

    assertThat(fsA1.getDelegate()).isSameAs(fsA2.getDelegate());
    assertThat(fsA1.getDelegate()).isNotSameAs(fsB.getDelegate());
  }

  @Test
  void evictedEntryClosesCachedDelegate() throws Exception {
    FileSystem mockFs = mock(FileSystem.class);
    FileSystemCredId key = FileSystemCredId.create(tableConf("tid-evict", "READ"));
    CredScopedFileSystem.CACHE.put(key, mockFs);

    CredScopedFileSystem.clearCacheForTesting();

    verify(mockFs).close();
  }
}
