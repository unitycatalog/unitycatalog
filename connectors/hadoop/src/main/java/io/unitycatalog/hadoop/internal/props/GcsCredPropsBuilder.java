package io.unitycatalog.hadoop.internal.props;

import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.GcsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import org.apache.hadoop.conf.Configuration;

/** Builds GCS credential properties. */
final class GcsCredPropsBuilder extends CredPropsBuilder {
  private static final String GCS_VENDED_TOKEN_PROVIDER_CLASS =
      "io.unitycatalog.hadoop.internal.auth.GcsVendedTokenProvider";
  private static final String GCS_ACCESS_TOKEN_KEY = "fs.gs.auth.access.token.credential";
  private static final String GCS_ACCESS_TOKEN_EXPIRATION_KEY =
      "fs.gs.auth.access.token.expiration";
  private static final String GCS_CONFLICT_CHECK_KEY = "fs.gs.create.items.conflict.check.enable";

  GcsCredPropsBuilder(Configuration conf) {
    super(conf);
    set("fs.gs.impl.disable.cache", "true");
    // The upstream GCS connector defaults this to true which causes the connector to
    // stat every ancestor directory on file creation. With UC-vended downscoped tokens
    // (scoped to a table's path prefix) these ancestor stats return 403. Default to
    // false; users with broader credentials can opt back in via Hadoop/Spark config.
    set(GCS_CONFLICT_CHECK_KEY, conf.get(GCS_CONFLICT_CHECK_KEY, "false"));
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
    set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER");
    set("fs.gs.auth.access.token.provider", GCS_VENDED_TOKEN_PROVIDER_CLASS);
  }

  @Override
  protected void setRenewableCredKeys(GenericCredential cred) {
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
  protected void setFixedCredKeys(GenericCredential cred) {
    Preconditions.checkArgument(
        cred instanceof GcsCredential,
        "Expected GcsCredential, but got %s",
        cred.getClass().getSimpleName());
    GcsCredential gcs = (GcsCredential) cred;
    Long expirationTime =
        gcs.expirationTimeMillis() == null ? Long.MAX_VALUE : gcs.expirationTimeMillis();
    set(GCS_ACCESS_TOKEN_KEY, gcs.oauthToken());
    set(GCS_ACCESS_TOKEN_EXPIRATION_KEY, String.valueOf(expirationTime));
  }
}
