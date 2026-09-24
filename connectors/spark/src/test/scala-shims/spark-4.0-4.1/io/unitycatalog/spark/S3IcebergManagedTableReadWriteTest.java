package io.unitycatalog.spark;

/**
 * Runs the {@link IcebergManagedTableReadWriteTest} suite with the managed storage root on
 * emulated S3: the server's Iceberg metadata IO and Spark's {@code S3FileIO} (with its mock S3
 * client) both run against one shared local directory and validate the credentials UC vends. The
 * base classes supply all the wiring; this class only switches the storage scheme.
 */
public class S3IcebergManagedTableReadWriteTest extends IcebergManagedTableReadWriteTest {

  @Override
  protected String managedStorageCloudScheme() {
    return "s3";
  }
}
