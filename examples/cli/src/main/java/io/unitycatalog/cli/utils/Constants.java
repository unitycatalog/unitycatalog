package io.unitycatalog.cli.utils;

import java.util.Set;

public class Constants {
  public static final String URI_SCHEME_ABFS = "abfs";
  public static final String URI_SCHEME_ABFSS = "abfss";
  public static final String URI_SCHEME_GS = "gs";
  public static final String URI_SCHEME_S3 = "s3";
  public static final Set<String> SUPPORTED_CLOUD_SCHEMES =
      Set.of(URI_SCHEME_S3, URI_SCHEME_GS, URI_SCHEME_ABFSS, URI_SCHEME_ABFS);
  public static final String URI_SCHEME_FILE = "file";
}
