package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.util.regex.Pattern;

public class ValidationUtils {
  // Regex to allow only alphanumeric characters, underscores, hyphens, and @ signs
  private static final Pattern VALID_FORMAT = Pattern.compile("[a-zA-Z0-9_@-]+");
  private static final Integer MAX_NAME_LENGTH = 255;

  public static void validateSqlObjectName(String name) {
    if (name == null || name.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Name cannot be empty");
    }
    if (name.length() > MAX_NAME_LENGTH) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Name cannot be longer than " + MAX_NAME_LENGTH + " characters");
    }
    if (!VALID_FORMAT.matcher(name).matches()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Name cannot contain a period, space, forward-slash, or control characters");
    }
  }

  /**
   * Checks that the specified condition is true. If not, throws a BaseException with
   * INVALID_ARGUMENT error code and the specified message.
   *
   * <p>This method is similar to Guava's Preconditions.checkArgument but throws BaseException
   * instead of IllegalArgumentException.
   *
   * @param condition the condition to check
   * @param message the exception message to use if the check fails
   * @throws BaseException with INVALID_ARGUMENT if condition is false
   */
  public static void checkArgument(boolean condition, String message) {
    if (!condition) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, message);
    }
  }

  /**
   * Checks that the specified condition is true. If not, throws a BaseException with
   * INVALID_ARGUMENT error code and a formatted message.
   *
   * @param condition the condition to check
   * @param messageTemplate the template for the exception message with %s placeholders
   * @param messageArgs the arguments to be substituted into the message template
   * @throws BaseException with INVALID_ARGUMENT if condition is false
   */
  public static void checkArgument(
      boolean condition, String messageTemplate, Object... messageArgs) {
    if (!condition) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, String.format(messageTemplate, messageArgs));
    }
  }
}
