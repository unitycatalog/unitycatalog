package io.unitycatalog.hadoop.internal.props;

import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import org.apache.hadoop.conf.Configuration;

/** Builds S3 credential properties. */
final class S3CredPropsBuilder extends CredPropsBuilder {
  private static final String AWS_VENDED_TOKEN_PROVIDER_CLASS =
      "io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider";
  private static final String S3A_ACCESS_KEY = "fs.s3a.access.key";
  private static final String S3A_SECRET_KEY = "fs.s3a.secret.key";
  private static final String S3A_SESSION_TOKEN = "fs.s3a.session.token";

  S3CredPropsBuilder(Configuration hadoopConf) {
    super(hadoopConf);
    set("fs.s3a.path.style.access", "true");
    set("fs.s3.impl.disable.cache", "true");
    set("fs.s3a.impl.disable.cache", "true");
  }

  @Override
  protected void setFsImplKeys() {
    saveAndOverride("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem", CRED_SCOPED_FS_CLASS);
    saveAndOverride("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem", CRED_SCOPED_FS_CLASS);
    saveAndOverride(
        "fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A", CRED_SCOPED_AFS_CLASS);
    saveAndOverride(
        "fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A", CRED_SCOPED_AFS_CLASS);
  }

  @Override
  protected void setVendedProviderKeys() {
    set(UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER, AWS_VENDED_TOKEN_PROVIDER_CLASS);
  }

  @Override
  protected void setRenewableCredKeys(GenericCredential cred) {
    Preconditions.checkArgument(
        cred instanceof AwsCredential,
        "Expected AwsCredential, but got %s",
        cred.getClass().getSimpleName());
    AwsCredential aws = (AwsCredential) cred;
    set(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, aws.accessKeyId());
    set(UCHadoopConfConstants.S3A_INIT_SECRET_KEY, aws.secretAccessKey());
    set(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, aws.sessionToken());
    // Expiration may be absent (e.g. a static token provider), so write the key only when set.
    if (aws.expirationTimeMillis() != null) {
      set(
          UCHadoopConfConstants.S3A_INIT_CRED_EXPIRED_TIME,
          String.valueOf(aws.expirationTimeMillis()));
    }
  }

  @Override
  protected void setFixedCredKeys(GenericCredential cred) {
    Preconditions.checkArgument(
        cred instanceof AwsCredential,
        "Expected AwsCredential, but got %s",
        cred.getClass().getSimpleName());
    AwsCredential aws = (AwsCredential) cred;
    set(S3A_ACCESS_KEY, aws.accessKeyId());
    set(S3A_SECRET_KEY, aws.secretAccessKey());
    set(S3A_SESSION_TOKEN, aws.sessionToken());
  }
}
