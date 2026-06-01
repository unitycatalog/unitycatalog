package io.unitycatalog.client.internal;

public class Preconditions {
  private Preconditions() {}

  public static void checkArgument(boolean expression, String message, Object... args) {
    if (!expression) {
      throw new IllegalArgumentException(String.format(message, args));
    }
  }

  public static <T> T checkNotNull(T object, String message, Object... args) {
    if (object == null) {
      throw new NullPointerException(String.format(message, args));
    }
    return object;
  }

  public static void checkState(boolean expression, String message, Object... args) {
    if (!expression) {
      throw new IllegalStateException(String.format(message, args));
    }
  }
}
