package io.unitycatalog.spark.auth.storage;

/**
 * Cache-enabled counterpart of {@link AwsCredRenewITTest}: runs the inherited renewal tests and the
 * server-cache-hit test with the server-side storage credential cache ON (sharing the connector's
 * manual clock via {@link TestClockProvider}). The cache-ON leads and the hit test live in {@link
 * BaseCredRenewITTest}, so every vendor's cache-ON class is identical to this.
 */
public class AwsCachedCredRenewITTest extends AwsCredRenewITTest {

  @Override
  protected boolean cacheEnabled() {
    return true;
  }
}
