package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.GcsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class GcsCredPropsTest extends CredPropsBaseTest {

  private static final String CONFLICT_CHECK_KEY = "fs.gs.create.items.conflict.check.enable";
  private static final String STATIC_EXPIRATION_KEY = "fs.gs.auth.access.token.expiration";
  private static final String CUSTOM_IMPL = "com.example.CustomGcsFileSystem";
  private static final String GHFS = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem";
  private static final String GHFS_AFS = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS";
  private static final String VENDED_PROVIDER =
      "io.unitycatalog.hadoop.internal.auth.GcsVendedTokenProvider";

  @Override
  String scheme() {
    return "gs";
  }

  @Override
  String location() {
    return "gs://bucket/data";
  }

  @Override
  GenericCredential vendedCred(Long expirationMillis, String prefix) {
    return new GcsCredential("token", expirationMillis, prefix);
  }

  @Override
  Map<String, String> defaultKeys() {
    return props(CONFLICT_CHECK_KEY, "false", "fs.gs.impl.disable.cache", "true");
  }

  /**
   * GCS is the sole cloud whose static path emits an expiration: {@link #STATIC_EXPIRATION_KEY} is
   * always set, defaulting to {@code Long.MAX_VALUE} when the credential has none.
   */
  @Override
  Map<String, String> staticCredKeys(Long expiration) {
    Map<String, String> keys = new HashMap<>();
    keys.put("fs.gs.auth.access.token.credential", "token");
    keys.put(
        STATIC_EXPIRATION_KEY, String.valueOf(expiration == null ? Long.MAX_VALUE : expiration));
    return keys;
  }

  @Override
  Map<String, String> initialCredKeys(Long expiration) {
    Map<String, String> keys = props(UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN, "token");
    if (expiration != null) {
      keys.put(
          UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME, String.valueOf(expiration));
    }
    return keys;
  }

  @Override
  Map<String, String> renewableProviderKeys() {
    return props(
        "fs.gs.auth.type",
        "ACCESS_TOKEN_PROVIDER",
        "fs.gs.auth.access.token.provider",
        VENDED_PROVIDER);
  }

  @Override
  Map<String, String> fileSystemImplKeys() {
    return props("fs.gs.impl", GHFS);
  }

  @Override
  Map<String, String> abstractFileSystemImplKeys() {
    return props("fs.AbstractFileSystem.gs.impl", GHFS_AFS);
  }

  @Override
  Map<String, String> customImplSeed() {
    return props("fs.gs.impl", CUSTOM_IMPL);
  }

  // ---- GCS-only tests ------

  @Test
  void conflictCheckDefaultsFalse() throws Exception {
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            false,
            new Configuration(false),
            scheme(),
            null,
            CATALOG_URI,
            tokenProvider(),
            TABLE_ID,
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props).containsEntry(CONFLICT_CHECK_KEY, "false");
  }

  @Test
  void conflictCheckRespectsUserOverrideToTrue() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(CONFLICT_CHECK_KEY, "true");

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            false,
            conf,
            scheme(),
            null,
            CATALOG_URI,
            tokenProvider(),
            TABLE_ID,
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props).containsEntry(CONFLICT_CHECK_KEY, "true");
  }

  @Test
  void conflictCheckDefaultsFalseAcrossPathAndDeltaCredProps() throws Exception {
    Map<String, String> pathProps =
        CredPropsUtil.createPathCredProps(
            false,
            false,
            new Configuration(false),
            scheme(),
            null,
            CATALOG_URI,
            tokenProvider(),
            location(),
            UCCredentialHadoopConfs.PathOperation.PATH_READ,
            Map.of());
    assertThat(pathProps).containsEntry(CONFLICT_CHECK_KEY, "false");

    Map<String, String> deltaProps =
        CredPropsUtil.createDeltaTableCredProps(
            false,
            false,
            new Configuration(false),
            scheme(),
            null,
            CATALOG_URI,
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            location(),
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());
    assertThat(deltaProps).containsEntry(CONFLICT_CHECK_KEY, "false");
  }
}
