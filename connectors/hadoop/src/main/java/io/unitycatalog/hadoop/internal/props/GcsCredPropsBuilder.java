package io.unitycatalog.hadoop.internal.props;

import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.GcsCredential;
import io.unitycatalog.hadoop.internal.auth.GcsFixedTokenProvider;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/** Builds GCS credential properties. */
final class GcsCredPropsBuilder extends CredPropsBuilder {
  private static final String GCS_AUTH_TYPE_KEY = "fs.gs.auth.type";
  private static final String GCS_AUTH_TYPE_ACCESS_TOKEN_PROVIDER = "ACCESS_TOKEN_PROVIDER";
  private static final String GCS_ACCESS_TOKEN_PROVIDER_KEY = "fs.gs.auth.access.token.provider";
  private static final String GCS_VENDED_TOKEN_PROVIDER_CLASS =
      "io.unitycatalog.hadoop.internal.auth.GcsVendedTokenProvider";
  private static final String GCS_FIXED_TOKEN_PROVIDER_CLASS =
      "io.unitycatalog.hadoop.internal.auth.GcsFixedTokenProvider";
  private static final String GCS_CONFLICT_CHECK_KEY = "fs.gs.create.items.conflict.check.enable";
  private static final String GCS_HNS_FOLDERS_KEY = "fs.gs.hierarchical.namespace.folders.enable";
  private static final String GCS_IMPLICIT_DIR_REPAIR_KEY = "fs.gs.implicit.dir.repair.enable";

  GcsCredPropsBuilder(Configuration conf) {
    super(conf);
    set("fs.gs.impl.disable.cache", "true");
    // Each setting makes the GCS connector call Google about more than the table's own path, which
    // a UC-vended downscoped token (scoped to a table's path prefix) is not allowed to do, so the
    // call fails:
    //   - the conflict check stats every ancestor folder when a file or folder is created; Hadoop
    //     3.5's core-default.xml turns it on;
    //   - the hierarchical namespace lookup reads the bucket's layout when a folder is deleted or
    //     renamed, which the parquet commit does; the connector turns it on itself, on every Spark
    //     version;
    //   - the implicit directory repair looks up the parent folder after a directory delete, such
    //     as the partition delete of INSERT OVERWRITE on a parquet table; the connector turns it on
    //     itself.
    // Default all three to false; users with broader credentials can opt back in via Hadoop/Spark
    // config.
    setOffUnlessEngineSets(GCS_CONFLICT_CHECK_KEY);
    setOffUnlessEngineSets(GCS_HNS_FOLDERS_KEY);
    setOffUnlessEngineSets(GCS_IMPLICIT_DIR_REPAIR_KEY);
  }

  /** Sets {@code key} to false unless the engine's configuration sets it. */
  private void setOffUnlessEngineSets(String key) {
    set(key, engineSets(key) ? hadoopConf().get(key) : "false");
  }

  @Override
  protected void setCredScopedFsKeys() {
    saveAndOverride(
        "fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        CRED_SCOPED_FS_CLASS);
    saveAndOverride(
        "fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        CRED_SCOPED_AFS_CLASS);
  }

  @Override
  protected void setVendedProviderKeys() {
    set(GCS_AUTH_TYPE_KEY, GCS_AUTH_TYPE_ACCESS_TOKEN_PROVIDER);
    set(GCS_ACCESS_TOKEN_PROVIDER_KEY, GCS_VENDED_TOKEN_PROVIDER_CLASS);
  }

  @Override
  protected void setInitRenewableCredKeys(GenericCredential cred) {
    Preconditions.checkArgument(
        cred instanceof GcsCredential,
        "Expected GcsCredential, but got %s",
        cred.getClass().getSimpleName());
    GcsCredential gcs = (GcsCredential) cred;
    set(UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN, gcs.oauthToken());
    // Expiration may be absent (e.g. a static token provider), so write the key only when set.
    if (gcs.expirationTimeMillis() != null) {
      set(
          UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME,
          String.valueOf(gcs.expirationTimeMillis()));
    }
  }

  @Override
  protected Map<String, String> initFixedCredKeys(GenericCredential cred) {
    Preconditions.checkArgument(
        cred instanceof GcsCredential,
        "Expected GcsCredential, but got %s",
        cred.getClass().getSimpleName());
    GcsCredential gcs = (GcsCredential) cred;
    Long expirationTime =
        gcs.expirationTimeMillis() == null
            ? GcsFixedTokenProvider.NO_EXPIRATION_MILLIS
            : gcs.expirationTimeMillis();
    return Map.of(
        GCS_AUTH_TYPE_KEY,
        GCS_AUTH_TYPE_ACCESS_TOKEN_PROVIDER,
        GCS_ACCESS_TOKEN_PROVIDER_KEY,
        GCS_FIXED_TOKEN_PROVIDER_CLASS,
        GcsFixedTokenProvider.ACCESS_TOKEN_KEY,
        gcs.oauthToken(),
        GcsFixedTokenProvider.ACCESS_TOKEN_EXPIRATION_KEY,
        String.valueOf(expirationTime));
  }
}
