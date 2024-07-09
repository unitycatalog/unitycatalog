package io.unitycatalog.server.service;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.utils.TemporaryCredentialUtils;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryTableCredentialsService {

    private static final TableRepository TABLE_REPOSITORY = TableRepository.getInstance();

    @Post("")
    public HttpResponse generateTemporaryTableCredential(GenerateTemporaryTableCredential generateTemporaryTableCredential) {
        String tableId = generateTemporaryTableCredential.getTableId();

        // Check if table exists
        String tableStorageLocation = "";
        TableInfo tableInfo = TABLE_REPOSITORY.getTableById(tableId);
        tableStorageLocation = tableInfo.getStorageLocation();

        // Generate temporary credentials
        if (tableStorageLocation == null || tableStorageLocation.isEmpty()) {
            throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Table storage location not found.");
        }
        if (tableStorageLocation.startsWith("s3://")) {
            return HttpResponse.ofJson(new GenerateTemporaryTableCredentialResponse()
               .awsTempCredentials(TemporaryCredentialUtils.findS3BucketConfig(tableStorageLocation)));

        } else {
            // return empty credentials for local file system
            return HttpResponse.ofJson(new GenerateTemporaryTableCredentialResponse());
        }
    }

}
