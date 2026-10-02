package io.unitycatalog.spark.auth.storage;

/**
 * Cache-enabled counterpart of {@link GcsCredRenewITTest}: runs the inherited renewal tests and the
 * server-cache-hit test with the server-side storage credential cache ON, over the GCS vend path.
 * The cache-ON leads and the hit test live in {@link BaseCredRenewITTest}, shared across vendors.
 */
public class GcsCachedCredRenewITTest extends GcsCredRenewITTest {

  @Override
  protected boolean cacheEnabled() {
    return true;
  }
}
