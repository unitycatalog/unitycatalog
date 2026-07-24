package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.GcsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/**
 * GCS realization of the {@link CredPropsBaseTest} matrix, plus the GCS-only conflict-check knob.
 */
class GcsCredPropsTest extends CredPropsBaseTest {

  private static final String CONFLICT_CHECK_KEY = "fs.gs.create.items.conflict.check.enable";
  private static final String STATIC_EXPIRATION_KEY = "fs.gs.auth.access.token.expiration";
  private static final String CUSTOM_IMPL = "com.example.CustomGcsFileSystem";
  private static final String GHFS = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem";
  private static final String GHFS_AFS = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS";
  private static final String VENDED_PROVIDER =
      "io.unitycatalog.hadoop.internal.auth.GcsVendedTokenProvider";
  private static final String MAX = String.valueOf(Long.MAX_VALUE);

  @Override
  String scheme() {
    return "gs";
  }

  @Override
  String location() {
    return "gs://bucket/data";
  }

  @Override
  GenericCredential vendedCred(Long expirationMillis) {
    return new GcsCredential("token", expirationMillis);
  }

  @Override
  String initExpirationKey() {
    return UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME;
  }

  /**
   * GCS is the sole cloud whose static path emits an expiration: {@code
   * fs.gs.auth.access.token.expiration} is always set, defaulting to {@code Long.MAX_VALUE} when
   * the credential has none. The renewable path follows the shared rule (init-expiration key iff
   * the credential carries an expiration).
   */
  @Override
  Map<String, String> expirationKeys(boolean renew, Long expirationMillis) {
    if (renew) {
      return super.expirationKeys(renew, expirationMillis);
    }
    return Map.of(
        STATIC_EXPIRATION_KEY, expirationMillis == null ? MAX : String.valueOf(expirationMillis));
  }

  @Override
  Map<String, String> constructorKeys() {
    return props(CONFLICT_CHECK_KEY, "false", "fs.gs.impl.disable.cache", "true");
  }

  @Override
  Map<String, String> implOverrideKeys() {
    return props(
        "fs.gs.impl", CRED_SCOPED_FS,
        "fs.gs.impl.original", GHFS,
        "fs.AbstractFileSystem.gs.impl", CRED_SCOPED_AFS,
        "fs.AbstractFileSystem.gs.impl.original", GHFS_AFS);
  }

  // Expiration keys are supplied by expirationKeys(), not baked in here.
  @Override
  Map<String, String> staticCredKeys() {
    return props("fs.gs.auth.access.token.credential", "token");
  }

  @Override
  Map<String, String> renewableCredKeys() {
    return props(
        "fs.gs.auth.type",
        "ACCESS_TOKEN_PROVIDER",
        "fs.gs.auth.access.token.provider",
        VENDED_PROVIDER,
        UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN,
        "token");
  }

  @Override
  Map<String, String> customImplSeed() {
    return props("fs.gs.impl", CUSTOM_IMPL);
  }

  @Override
  Map<String, String> customImplOriginals() {
    return props("fs.gs.impl.original", CUSTOM_IMPL);
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
