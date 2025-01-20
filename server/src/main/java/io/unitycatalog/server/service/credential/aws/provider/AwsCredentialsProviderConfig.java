package io.unitycatalog.server.service.credential.aws.provider;

import java.util.HashMap;
import java.util.Map;

public class AwsCredentialsProviderConfig {

  public static final String PROVIDER_CLASS = "class";
  public static final String ACCESS_KEY_ID = "access_key";
  public static final String SECRET_ACCESS_KEY = "secret_key";

  private String providerClass;
  private final Map<String, String> properties;

  public AwsCredentialsProviderConfig() {
    this.providerClass = "io.unitycatalog.server.service.credential.aws.provider.StsCredentialsProvider";
    this.properties = new HashMap<>();
  }

  public void put(String name, String value) {
    if (!name.equals(PROVIDER_CLASS)) {
      this.properties.put(name, value);
    } else {
      this.providerClass = value;
    }
  }

  public String get(String name) {
    return this.properties.get(name);
  }

  public String get(String name, String defaultValue) {
    return this.properties.getOrDefault(name, defaultValue);
  }

  public String getProviderClass() {
    return providerClass;
  }
}
