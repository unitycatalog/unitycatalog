package io.unitycatalog.client;

public class Preconditions {
  private Preconditions() {}

  public static void checkNotNull(Object object, String errorMessage, Object... args) {
    if (object == null) {
      throw new NullPointerException(String.format(errorMessage, args));
    }
  }

  public static void checkArgument(boolean expression, String errorMessage, Object... args) {
    if (!expression) {
      throw new IllegalArgumentException(String.format(errorMessage, args));
    }
  }
}
