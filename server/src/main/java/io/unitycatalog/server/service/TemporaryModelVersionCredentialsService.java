package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.ModelRepository;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.utils.TemporaryCredentialUtils;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryModelVersionCredentialsService {

    private static final ModelRepository MODEL_REPOSITORY = ModelRepository.getInstance();

    @Post("")
    public HttpResponse generateTemporaryTableCredential(
            GenerateTemporaryModelVersionCredential generateTemporaryModelVersionCredential) {

        long modelVersion = generateTemporaryModelVersionCredential.getVersion();
        String catalogName = generateTemporaryModelVersionCredential.getCatalogName();
        String schemaName = generateTemporaryModelVersionCredential.getSchemaName();
        String modelName = generateTemporaryModelVersionCredential.getModelName();
        ModelVersionOperation operation = generateTemporaryModelVersionCredential.getOperation();
        String fullName = RepositoryUtils.getAssetFullName(catalogName, schemaName, modelName);

        // Check if model version exists
        ModelVersionInfo mvInfo = MODEL_REPOSITORY.getModelVersion(fullName, modelVersion);
        if (mvInfo == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Model version not found: " + fullName + "/" + modelVersion);
        }
        String mvStorageLocation = mvInfo.getStorageLocation();

        // Generate temporary credentials
        if (mvStorageLocation == null || mvStorageLocation.isEmpty()) {
            throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Model version storage location not found:" + fullName + "/" + modelVersion);
        }
        if (mvStorageLocation.startsWith("s3://")) {
            return HttpResponse.ofJson(
                    new GenerateTemporaryModelVersionCredentialResponse()
                            .awsTempCredentials(
                                    TemporaryCredentialUtils.findS3BucketConfig(mvStorageLocation)));

        } else {
            // return empty credentials for local file system
            return HttpResponse.ofJson(new GenerateTemporaryModelVersionCredentialResponse());
        }
    }
}
