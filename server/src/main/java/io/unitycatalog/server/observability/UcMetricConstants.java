package io.unitycatalog.server.observability;

/**
 * The parts UC metric names and tags are built from. A namespace joins segments with {@link
 * #DELIMITER}, and metrics in the same area share it as their prefix. Prometheus replaces the
 * delimiter with an underscore, so {@code uc.storage.credential.cache.requests} is scraped as
 * {@code uc_storage_credential_cache_requests_total}.
 */
public final class UcMetricConstants {
  /** Micrometer separates the parts of a meter name with a dot. */
  static final String DELIMITER = ".";

  static final String UC = "uc";
  static final String SECURABLE = "securable";
  static final String TABLE = "table";
  static final String STORAGE = "storage";
  static final String CREDENTIAL = "credential";
  static final String CACHE = "cache";
  static final String CLEANUP = "cleanup";

  static final String CREATIONS = "creations";
  static final String REQUESTS = "requests";
  static final String ATTEMPTS = "attempts";

  static final String UC_SECURABLE = UC + DELIMITER + SECURABLE;
  static final String UC_SECURABLE_TABLE = UC_SECURABLE + DELIMITER + TABLE;
  static final String UC_STORAGE = UC + DELIMITER + STORAGE;
  static final String UC_STORAGE_CREDENTIAL_CACHE =
      UC_STORAGE + DELIMITER + CREDENTIAL + DELIMITER + CACHE;
  static final String UC_STORAGE_CLEANUP = UC_STORAGE + DELIMITER + CLEANUP;

  /**
   * The outcome of an event: whether a cache served a request ({@link #HIT}) or could not ({@link
   * #MISS}), or whether a cleanup attempt succeeded ({@link #SUCCESS}) or failed ({@link
   * #FAILURE}).
   */
  public static final String RESULT = "result";

  public static final String HIT = "hit";
  public static final String MISS = "miss";
  public static final String SUCCESS = "success";
  public static final String FAILURE = "failure";

  /** The kind of managed resource a storage cleanup task removes, such as {@code volume}. */
  public static final String RESOURCE_TYPE = "resource_type";

  private UcMetricConstants() {}
}
