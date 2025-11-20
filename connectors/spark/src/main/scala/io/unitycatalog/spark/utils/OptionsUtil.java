package io.unitycatalog.spark.utils;

import java.util.Map;

public class OptionsUtil {
  private OptionsUtil() {
  }

  public static final String URI = "uri";
  public static final String TOKEN = "token";
  public static final String WAREHOUSE = "warehouse";
  public static final String OAUTH_URI = "oauthUri";
  public static final String OAUTH_CLIENT_ID = "oauthClientId";
  public static final String OAUTH_CLIENT_SECRET = "oauthClientSecret";

  public static final String RENEW_CREDENTIAL_ENABLED = "renewCredential.enabled";
  public static final boolean DEFAULT_RENEW_CREDENTIAL_ENABLED = false;

  public static boolean getBoolean(
      Map<String, String> props, String property, boolean defaultValue) {
    String value = props.get(property);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return defaultValue;
  }
}
