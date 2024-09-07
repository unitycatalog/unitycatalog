package io.unitycatalog.server.utils;

import java.util.Set;

public class Constants {
  public static final String CATALOG = "catalog";
  public static final String SCHEMA = "schema";
  public static final String TABLE = "table";
  public static final String FUNCTION = "function";

  public static final String URI_SCHEME_ABFS = "abfs";
  public static final String URI_SCHEME_ABFSS = "abfss";
  public static final String URI_SCHEME_GS = "gs";
  public static final String URI_SCHEME_S3 = "s3";
  public static final Set<String> SUPPORTED_SCHEMES =
      Set.of(URI_SCHEME_S3, URI_SCHEME_GS, URI_SCHEME_ABFSS, URI_SCHEME_ABFS);
}
