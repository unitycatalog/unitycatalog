package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.*;
import com.linecorp.armeria.common.HttpResponse;
import io.unitycatalog.server.persist.VolumeRepository;
import io.unitycatalog.server.exception.ErrorCode;

import java.util.regex.Pattern;

public class ValidationUtils {
    public static final String CATALOG = "Catalog";
    public static final String SCHEMA = "Schema";
    public static final String FUNCTION = "Function";

    // Regex to reject names containing a period, space, forward-slash, C0 + DEL control characters
    private static final Pattern INVALID_FORMAT = Pattern.compile("[\\.\\ \\/\\x00-\\x1F\\x7F]");
    private static final Integer MAX_NAME_LENGTH = 255;

    public static void validateSqlObjectName(String name) {
        if (name == null || name.isEmpty()) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Name cannot be empty");
        }
        if (name.length() > MAX_NAME_LENGTH) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Name cannot be longer than "+ MAX_NAME_LENGTH + " characters");
        }
        if (INVALID_FORMAT.matcher(name).find()) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Name cannot contain a period, space, forward-slash, or control characters");
        }
    }
}