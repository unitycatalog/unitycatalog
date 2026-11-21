package io.unitycatalog.server.utils;

public class Constants {
  public static final String CATALOG = "catalog";
  public static final String SCHEMA = "schema";
  public static final String TABLE = "table";
  public static final String FUNCTION = "function";

  public static final String MANAGED_STORAGE_PREFIX = "__unitystorage";
  public static final String MANAGED_STORAGE_CATALOG_PREFIX = MANAGED_STORAGE_PREFIX + "/catalogs";
  public static final String MANAGED_STORAGE_SCHEMA_PREFIX = MANAGED_STORAGE_PREFIX + "/schemas";

  /**
   * The width of the storage location columns, in characters. {@code StorageLocationValidator}
   * holds a location given in a request to it in characters, and a location the server builds in
   * UTF-8 bytes.
   */
  public static final int MAX_STORAGE_LOCATION_LENGTH = 2048;

  /** The width of the registered model and model version location columns, in characters. */
  public static final int MAX_MODEL_LOCATION_LENGTH = 4096;

  /** The width of the external location URL column, in characters. */
  public static final int MAX_EXTERNAL_LOCATION_URL_LENGTH = 255;
}
