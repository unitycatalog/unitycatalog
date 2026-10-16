package io.unitycatalog.server.observability;

/**
 * The parts UC metric names and tags are built from. A namespace joins segments with {@link
 * #DELIMITER}, and metrics in the same area share it as their prefix. Prometheus replaces the
 * delimiter with an underscore, so {@code uc.storage.credential.cache.requests} is scraped as
 * {@code uc_storage_credential_cache_requests_total}.
 */
public final class UcMetricConstants {
  /** Micrometer separates the parts of a meter name with a dot. */
  public static final String DELIMITER = ".";

  public static final String UC = "uc";
  public static final String SECURABLE = "securable";
  public static final String TABLE = "table";
  public static final String STORAGE = "storage";
  public static final String CREDENTIAL = "credential";
  public static final String CACHE = "cache";

  public static final String CREATIONS = "creations";
  public static final String REQUESTS = "requests";

  public static final String UC_SECURABLE = UC + DELIMITER + SECURABLE;
  public static final String UC_SECURABLE_TABLE = UC_SECURABLE + DELIMITER + TABLE;
  public static final String UC_STORAGE = UC + DELIMITER + STORAGE;
  public static final String UC_STORAGE_CREDENTIAL_CACHE =
      UC_STORAGE + DELIMITER + CREDENTIAL + DELIMITER + CACHE;

  /** Whether a cache served a request ({@link #HIT}) or could not ({@link #MISS}). */
  public static final String RESULT = "result";

  public static final String HIT = "hit";
  public static final String MISS = "miss";

  private UcMetricConstants() {}
}
