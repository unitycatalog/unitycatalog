package io.unitycatalog.hadoop.internal;

import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import java.util.Map;

/** S3 realization of the {@link CredPropsBaseTest} matrix. */
class S3CredPropsTest extends CredPropsBaseTest {

  private static final String CUSTOM_IMPL = "com.example.CustomS3FileSystem";
  private static final String S3A_FS = "org.apache.hadoop.fs.s3a.S3AFileSystem";
  private static final String S3A_AFS = "org.apache.hadoop.fs.s3a.S3A";
  private static final String VENDED_PROVIDER =
      "io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider";

  @Override
  String scheme() {
    return "s3";
  }

  @Override
  String location() {
    return "s3://bucket/data";
  }

  @Override
  GenericCredential vendedCred(Long expirationMillis) {
    return new AwsCredential("ak", "sk", "st", expirationMillis);
  }

  @Override
  String initExpirationKey() {
    return UCHadoopConfConstants.S3A_INIT_CRED_EXPIRED_TIME;
  }

  @Override
  Map<String, String> constructorKeys() {
    return props(
        "fs.s3a.path.style.access", "true",
        "fs.s3.impl.disable.cache", "true",
        "fs.s3a.impl.disable.cache", "true");
  }

  @Override
  Map<String, String> implOverrideKeys() {
    return props(
        "fs.s3.impl", CRED_SCOPED_FS,
        "fs.s3.impl.original", S3A_FS,
        "fs.s3a.impl", CRED_SCOPED_FS,
        "fs.s3a.impl.original", S3A_FS,
        "fs.AbstractFileSystem.s3.impl", CRED_SCOPED_AFS,
        "fs.AbstractFileSystem.s3.impl.original", S3A_AFS,
        "fs.AbstractFileSystem.s3a.impl", CRED_SCOPED_AFS,
        "fs.AbstractFileSystem.s3a.impl.original", S3A_AFS);
  }

  @Override
  Map<String, String> staticCredKeys() {
    return props(
        "fs.s3a.access.key", "ak",
        "fs.s3a.secret.key", "sk",
        "fs.s3a.session.token", "st");
  }

  @Override
  Map<String, String> renewableCredKeys() {
    return props(
        UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER, VENDED_PROVIDER,
        UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak",
        UCHadoopConfConstants.S3A_INIT_SECRET_KEY, "sk",
        UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, "st");
  }

  @Override
  Map<String, String> customImplSeed() {
    return props("fs.s3.impl", CUSTOM_IMPL, "fs.s3a.impl", CUSTOM_IMPL);
  }

  @Override
  Map<String, String> customImplOriginals() {
    return props("fs.s3.impl.original", CUSTOM_IMPL, "fs.s3a.impl.original", CUSTOM_IMPL);
  }
}
