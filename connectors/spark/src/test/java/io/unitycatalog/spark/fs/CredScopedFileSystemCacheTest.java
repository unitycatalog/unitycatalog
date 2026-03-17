package io.unitycatalog.spark.fs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.unitycatalog.spark.UCHadoopConf;
import java.net.URI;
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
    Configuration conf = new Configuration();
    conf.set(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY, UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConf.UC_TABLE_ID_KEY, tableId);
    conf.set(UCHadoopConf.UC_TABLE_OPERATION_KEY, op);
    return conf;
  }

  @Test
  void sameScope_reusesSameDelegate() throws Exception {
    URI uri = new URI("file:///tmp");
    Configuration conf = tableConf("tid-1", "READ");

    CredScopedFileSystem fs1 = init(uri, conf);
    CredScopedFileSystem fs2 = init(uri, conf);

    assertThat(fs1.delegate).isSameAs(fs2.delegate);
  }

  @Test
  void differentScope_getsDifferentDelegate() throws Exception {
    URI uri = new URI("file:///tmp");

    CredScopedFileSystem fsRead = init(uri, tableConf("tid-1", "READ"));
    CredScopedFileSystem fsWrite = init(uri, tableConf("tid-1", "WRITE"));

    assertThat(fsRead.delegate).isNotSameAs(fsWrite.delegate);
  }

  @Test
  void evictedEntry_closesCachedDelegate() throws Exception {
    // Pre-seed the cache with a mock delegate so we can verify close() is called.
    FileSystem mockFs = mock(FileSystem.class);
    CredScopedKey key = new CredScopedKey.TableCredScopedKey("tid-evict", "READ");
    CredScopedFileSystem.CACHE.put(key, mockFs);

    // Invalidate (simulates LRU eviction) and flush the removal listener.
    CredScopedFileSystem.clearCacheForTesting();

    verify(mockFs).close();
  }
}
