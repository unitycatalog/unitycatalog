package io.unitycatalog.hadoop.internal;

import io.unitycatalog.client.internal.Preconditions;

public final class CredentialUtil {
  private CredentialUtil() {}

  public static String field(String value, String message, Object... args) {
    Preconditions.checkArgument(value != null && !value.isEmpty(), message, args);
    return value;
  }
}
