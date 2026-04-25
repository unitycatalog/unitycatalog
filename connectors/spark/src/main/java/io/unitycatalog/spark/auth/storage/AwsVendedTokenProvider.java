package io.unitycatalog.spark.auth.storage;

import org.apache.hadoop.conf.Configuration;

/** Compatibility wrapper for the shared UC credential provider implementation. */
public class AwsVendedTokenProvider extends io.unitycatalog.client.storage.AwsVendedTokenProvider {
  public AwsVendedTokenProvider(Configuration conf) {
    super(conf);
  }
}
