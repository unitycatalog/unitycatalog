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
}
