package io.unitycatalog.client.internal;

public class Preconditions {
  private Preconditions() {}

  public static void checkArgument(boolean expression, String message, Object... args) {
    if (!expression) {
      throw new IllegalArgumentException(String.format(message, args));
    }
  }

  public static void checkNotNull(Object object, String message, Object... args) {
    if (object == null) {
      throw new NullPointerException(String.format(message, args));
    }
  }
}
