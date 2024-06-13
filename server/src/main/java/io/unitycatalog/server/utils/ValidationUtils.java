package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.FunctionRepository;
import io.unitycatalog.server.persist.SchemaOperations;
import com.linecorp.armeria.common.HttpResponse;
import io.unitycatalog.server.persist.CatalogOperations;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.VolumeOperations;
import io.unitycatalog.server.exception.ErrorCode;

import java.util.Optional;
import java.util.regex.Pattern;

public class ValidationUtils {
    public static final String CATALOG = "Catalog";
    public static final String SCHEMA = "Schema";
    public static final String VOLUME = "Volume";
    public static final String FUNCTION = "Function";
    private static final CatalogOperations CATALOG_OPERATIONS = CatalogOperations.getInstance();
    private static final SchemaOperations SCHEMA_OPERATIONS = SchemaOperations.getInstance();
    private static final VolumeOperations VOLUME_OPERATIONS = VolumeOperations.getInstance();
    private static final FunctionRepository FUNCTION_REPOSITORY = FunctionRepository.getInstance();
    private static final TableRepository TABLE_REPOSITORY = TableRepository.getInstance();

    // Regex to reject names containing a period, space, forward-slash, C0 + DEL control characters
    private static final Pattern INVALID_FORMAT = Pattern.compile("[\\.\\ \\/\\x00-\\x1F\\x7F]");
    private static final Integer MAX_NAME_LENGTH = 255;

    public static HttpResponse entityNotFoundResponse(String entity, String... message) {
        throw new BaseException(ErrorCode.NOT_FOUND, entity + " not found: " + String.join(".", message));
    }

    public static HttpResponse entityAlreadyExistsResponse(String entity, String... message) {
        throw new BaseException(ErrorCode.ALREADY_EXISTS, entity + " already exists: " + String.join(".", message));
    }

    public static boolean catalogExists(String catalogName) {
        return CATALOG_OPERATIONS.getCatalog(catalogName) != null;
    }

    public static boolean schemaExists(String catalogName, String schemaName) {
        if (!catalogExists(catalogName))
            return false;
        return SCHEMA_OPERATIONS.getSchema(catalogName + "." + schemaName) != null;
    }

    public static boolean volumeExists(String catalogName, String schemaName, String volumeName) {
        if (!schemaExists(catalogName, schemaName))
            return false;
        return VOLUME_OPERATIONS.getVolume(catalogName + "." + schemaName + "." + volumeName) != null;
    }

    public static boolean functionExists(String catalogName, String schemaName, String functionName) {
        if (!schemaExists(catalogName, schemaName))
            return false;
        return FUNCTION_REPOSITORY.getFunction(catalogName + "." + schemaName + "." + functionName) != null;
    }

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
