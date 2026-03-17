package io.unitycatalog.spark.utils;

import java.util.Map;

public class OptionsUtil {
  private OptionsUtil() {}

  public static final String URI = "uri";
  public static final String TOKEN = "token";
  public static final String WAREHOUSE = "warehouse";

  public static final String RENEW_CREDENTIAL_ENABLED = "renewCredential.enabled";
  public static final boolean DEFAULT_RENEW_CREDENTIAL_ENABLED = true;

  /**
   * When {@code true}, wraps the cloud filesystem with {@link
   * io.unitycatalog.spark.fs.CredScopedFileSystem} to enable per-credential filesystem caching.
   * Instead of sharing one filesystem instance per bucket (Hadoop's default), each unique
   * credential scope gets its own cached instance, so the same filesystem is fully reused for all
   * file accesses within that scope without creating a new instance for every access. Disabled by
   * default; will be enabled by default in a future release.
   */
  public static final String CRED_SCOPED_FS_ENABLED = "credScopedFs.enabled";
  public static final boolean DEFAULT_CRED_SCOPED_FS_ENABLED = false;
  public static final String SERVER_SIDE_PLANNING_ENABLED = "serverSidePlanning.enabled";
  public static final boolean DEFAULT_SERVER_SIDE_PLANNING_ENABLED = false;

  public static boolean getBoolean(
      Map<String, String> props, String property, boolean defaultValue) {
    String value = props.get(property);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return defaultValue;
  }
}
