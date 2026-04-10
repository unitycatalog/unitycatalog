package io.unitycatalog.spark.utils;

import java.util.Map;

public class OptionsUtil {
  private OptionsUtil() {}

  public static final String URI = "uri";
  public static final String TOKEN = "token";
  public static final String WAREHOUSE = "warehouse";

  public static final String RENEW_CREDENTIAL_ENABLED = "renewCredential.enabled";
  public static final boolean DEFAULT_RENEW_CREDENTIAL_ENABLED = true;

  public static final String CRED_SCOPED_FS_ENABLED = "credScopedFs.enabled";
  public static final boolean DEFAULT_CRED_SCOPED_FS_ENABLED = false;
  public static final String SERVER_SIDE_PLANNING_ENABLED = "serverSidePlanning.enabled";
  public static final boolean DEFAULT_SERVER_SIDE_PLANNING_ENABLED = false;

  public static final String DELTA_REST_API_ENABLED = "deltaRestApi.enabled";
  public static final String DEFAULT_DELTA_REST_API_ENABLED = "false";

  public static boolean getBoolean(
      Map<String, String> props, String property, boolean defaultValue) {
    String value = props.get(property);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return defaultValue;
  }

  /**
   * Returns the tri-state value of a property: "true", "false", or "auto". Defaults to the provided
   * default if the property is not set.
   */
  public static String getTriState(
      Map<String, String> props, String property, String defaultValue) {
    String value = props.get(property);
    if (value != null) {
      return value.toLowerCase();
    }
    return defaultValue;
  }
}
