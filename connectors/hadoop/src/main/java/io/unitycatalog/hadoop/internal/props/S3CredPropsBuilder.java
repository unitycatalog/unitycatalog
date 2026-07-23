package io.unitycatalog.hadoop.internal.props;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import org.apache.hadoop.conf.Configuration;

/** Builds S3 credential properties. */
final class S3CredPropsBuilder extends CredPropsBuilder {
  private static final String AWS_VENDED_TOKEN_PROVIDER_CLASS =
      "io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider";

  S3CredPropsBuilder() {
    set("fs.s3a.path.style.access", "true");
    set("fs.s3.impl.disable.cache", "true");
    set("fs.s3a.impl.disable.cache", "true");
  }

  @Override
  protected void writeImplOverrides(Configuration conf) {
    saveAndOverride(
        conf, "fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem", CRED_SCOPED_FS_CLASS);
    saveAndOverride(
        conf, "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem", CRED_SCOPED_FS_CLASS);
    saveAndOverride(
        conf,
        "fs.AbstractFileSystem.s3.impl",
        "org.apache.hadoop.fs.s3a.S3A",
        CRED_SCOPED_AFS_CLASS);
    saveAndOverride(
        conf,
        "fs.AbstractFileSystem.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3A",
        CRED_SCOPED_AFS_CLASS);
  }

  @Override
  protected void applyVendedProviderKeys() {
    set(UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER, AWS_VENDED_TOKEN_PROVIDER_CLASS);
  }

  @Override
  public CredPropsBuilder writeCredKeys(boolean renewable, GenericCredential cred) {
    AwsCredential aws = (AwsCredential) cred;
    if (renewable) {
      set(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, aws.accessKeyId());
      set(UCHadoopConfConstants.S3A_INIT_SECRET_KEY, aws.secretAccessKey());
      set(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, aws.sessionToken());
      // Expiration may be absent (e.g. a static token provider), so write the key only when set.
      if (aws.expirationTimeMillis() != null) {
        set(
            UCHadoopConfConstants.S3A_INIT_CRED_EXPIRED_TIME,
            String.valueOf(aws.expirationTimeMillis()));
      }
    } else {
      set("fs.s3a.access.key", aws.accessKeyId());
      set("fs.s3a.secret.key", aws.secretAccessKey());
      set("fs.s3a.session.token", aws.sessionToken());
    }
    return this;
  }
}
