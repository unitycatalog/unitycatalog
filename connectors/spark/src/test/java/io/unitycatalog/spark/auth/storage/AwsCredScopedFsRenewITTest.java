package io.unitycatalog.spark.auth.storage;

import io.unitycatalog.spark.utils.OptionsUtil;
import java.util.Map;

/**
 * Re-runs the AWS credential-renewal test with {@code credScopedFs.enabled=true} to verify that
 * {@link io.unitycatalog.spark.fs.CredScopedFileSystem} wraps the delegate transparently and
 * credential renewal still works correctly end-to-end.
 *
 * <p>Sets {@code fs.s3.impl.original} at the session level so that {@code
 * CredScopedFileSystem#newFileSystem} restores {@link S3CredFileSystem} — rather than the default
 * {@code S3AFileSystem} — as the real delegate, exercising the side-channel restoration path.
 */
public class AwsCredScopedFsRenewITTest extends AwsCredRenewITTest {

  @Override
  protected Map<String, String> catalogExtraProps() {
    String catalogKey = "spark.sql.catalog." + CATALOG_NAME;
    return Map.of(
        // Enable CredScopedFileSystem wrapping for this catalog.
        catalogKey + "." + OptionsUtil.CRED_SCOPED_FS_ENABLED,
        "true",
        // Tell CredScopedFileSystem to restore S3CredFileSystem as the real delegate so the
        // credential-checking logic in the parent test still runs.
        "spark.hadoop.fs.s3.impl.original",
        S3CredFileSystem.class.getName(),
        "spark.hadoop.fs.s3a.impl.original",
        S3CredFileSystem.class.getName());
  }
}
