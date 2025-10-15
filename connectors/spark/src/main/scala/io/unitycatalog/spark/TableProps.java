package io.unitycatalog.spark;

import java.util.Map;

public class TableProps {
  private TableProps() {
  }

  public static final String FIXED_CREDENTIAL_ENABLED = "fixedCredential.enabled";
  public static final boolean DEFAULT_FIXED_CREDENTIAL_ENABLED = false;

  public static boolean getBoolean(
      Map<String, String> props, String property, boolean defaultValue) {
    String value = props.get(property);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return defaultValue;
  }
}
