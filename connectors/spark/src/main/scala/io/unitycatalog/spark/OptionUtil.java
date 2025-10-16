package io.unitycatalog.spark;

import java.util.Map;

public class OptionUtil {
  private OptionUtil() {
  }

  public static final String URI = "uri";
  public static final String TOKEN = "token";

  public static final String RENEW_CREDENTIAL_ENABLED = "renewCredential.enabled";
  public static final boolean DEFAULT_RENEW_CREDENTIAL_ENABLED = true;

  public static boolean getBoolean(
      Map<String, String> props, String property, boolean defaultValue) {
    String value = props.get(property);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return defaultValue;
  }
}
