package io.unitycatalog.server.utils;

public class Constants {
  public static final String CATALOG = "catalog";
  public static final String SCHEMA = "schema";
  public static final String TABLE = "table";
  public static final String FUNCTION = "function";

  public static final String MANAGED_STORAGE_PREFIX = "__unitystorage";
  public static final String MANAGED_STORAGE_CATALOG_SEGMENT = "catalogs";
  public static final String MANAGED_STORAGE_SCHEMA_SEGMENT = "schemas";
  public static final String MANAGED_STORAGE_CATALOG_PREFIX =
      MANAGED_STORAGE_PREFIX + "/" + MANAGED_STORAGE_CATALOG_SEGMENT;
  public static final String MANAGED_STORAGE_SCHEMA_PREFIX =
      MANAGED_STORAGE_PREFIX + "/" + MANAGED_STORAGE_SCHEMA_SEGMENT;
}
