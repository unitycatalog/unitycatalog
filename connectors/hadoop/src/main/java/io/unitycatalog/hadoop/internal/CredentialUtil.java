package io.unitycatalog.hadoop.internal;

import io.unitycatalog.client.internal.Preconditions;

public final class CredentialUtil {
  private CredentialUtil() {}

  public static String field(String value, String description, String label) {
    return Preconditions.requireArgument(
        value != null && !value.isEmpty(), value, "%s is missing %s.", description, label);
  }
}
