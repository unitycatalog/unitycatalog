package io.unitycatalog.hadoop.internal;

public final class CloudType {
  private CloudType() {}

  public static boolean isSupportedScheme(String scheme) {
    switch (scheme) {
      case "s3":
      case "gs":
      case "abfs":
      case "abfss":
        return true;
      default:
        return false;
    }
  }
}
