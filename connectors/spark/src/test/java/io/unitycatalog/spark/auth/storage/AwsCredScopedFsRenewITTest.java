package io.unitycatalog.spark.auth.storage;

import io.unitycatalog.spark.utils.OptionsUtil;
import java.util.Map;

/**
 * Re-runs the AWS credential-renewal test with {@code credScopedFs.enabled=true} to verify that
 * {@link io.unitycatalog.hadoop.internal.fs.CredScopedFileSystem} wraps the delegate transparently
 * and credential renewal still works correctly end-to-end.
 *
 * <p>Sets {@code fs.s3.impl} and {@code fs.s3a.impl} at the session level to {@link
 * S3CredFileSystem} so that {@code CredPropsUtil} reads them as the original impls and saves them
 * under {@code fs.s3.impl.original} / {@code fs.s3a.impl.original} before installing {@code
 * CredScopedFileSystem}. This ensures the credential-checking logic in the parent test still runs
 * through the real delegate.
 */
public class AwsCredScopedFsRenewITTest extends AwsCredRenewITTest {

  @Override
  protected Map<String, String> catalogExtraProps() {
    String catalogKey = "spark.sql.catalog." + CATALOG_NAME;
    return Map.of(
        catalogKey + "." + OptionsUtil.CRED_SCOPED_FS_ENABLED,
        "true",
        "spark.hadoop.fs.s3.impl",
        S3CredFileSystem.class.getName(),
        "spark.hadoop.fs.s3a.impl",
        S3CredFileSystem.class.getName());
  }
}
