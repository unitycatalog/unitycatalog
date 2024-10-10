package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.*;
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

  public static void validateNonEmpty(String value, String message) {
    if (value == null || value.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, message);
    }
  }

  public static void validateNonEmpty(Object value, String message) {
    if (value == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, message);
    }
  }

  public static void validateGreaterThanEqualTo(
      Long value, Long greaterThanEqualTo, String message) {
    if (value == null || value < greaterThanEqualTo) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, message);
    }
  }

  public static void validateGreaterThan(Long value, Long greaterThan, String message) {
    if (value == null || value <= greaterThan) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, message);
    }
  }

  public static void validateEquals(Object value1, Object value2, String message) {
    if (value1 == null || !value1.equals(value2)) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, message);
    }
  }

  public static void validateNotEquals(Object value1, Object value2, String message) {
    if (value1 == null || value1.equals(value2)) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, message);
    }
  }
}
