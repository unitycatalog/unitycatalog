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
import io.unitycatalog.server.service.credential.CredentialOperations;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.persist.ModelRepository;

import java.util.Collections;
import java.util.Set;

import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryModelVersionCredentialsService {

    private static final ModelRepository MODEL_REPOSITORY = ModelRepository.getInstance();

    private final CredentialOperations credentialOps;

    public TemporaryModelVersionCredentialsService(CredentialOperations credentialOps) {
        this.credentialOps = credentialOps;
    }

    @Post("")
    public HttpResponse generateTemporaryModelVersionCredentials(
            GenerateTemporaryModelVersionCredentials generateTemporaryModelVersionCredentials) {

        long modelVersion = generateTemporaryModelVersionCredentials.getVersion();
        String catalogName = generateTemporaryModelVersionCredentials.getCatalogName();
        String schemaName = generateTemporaryModelVersionCredentials.getSchemaName();
        String modelName = generateTemporaryModelVersionCredentials.getModelName();
        String fullName = RepositoryUtils.getAssetFullName(catalogName, schemaName, modelName);

        ModelVersionInfo modelVersionInfo = MODEL_REPOSITORY.getModelVersion(fullName, modelVersion);
        String storageLocation = modelVersionInfo.getStorageLocation();
        if (storageLocation.toLowerCase().startsWith("file")) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Cannot request credentials on a model version with a file based storage location: " + fullName + "/" + modelVersion);
        }
        ModelVersionOperation requestedOperation = generateTemporaryModelVersionCredentials.getOperation();
        // Must enforce that the status of the model version matches the requested credential type.
        if (modelVersionInfo.getStatus() == ModelVersionStatus.FAILED_REGISTRATION || modelVersionInfo.getStatus() == ModelVersionStatus.MODEL_VERSION_STATUS_UNKNOWN) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Cannot request credentials on a model version with status " + modelVersionInfo.getStatus().getValue() + ": " + fullName + "/" + modelVersion);
        }
        if ((modelVersionInfo.getStatus() != ModelVersionStatus.PENDING_REGISTRATION &&
                requestedOperation == ModelVersionOperation.READ_WRITE_MODEL_VERSION)) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Cannot request read/write credentials on a model version that has been finalized: " + fullName + "/" + modelVersion);
        }
        return HttpResponse.ofJson(
                credentialOps.vendCredentialForPath(
                        modelVersionInfo.getStorageLocation(),
                        modelVersionOperationToPrivileges(requestedOperation))
                        .toModelVersionCredentialsResponse());
    }

    private Set<CredentialContext.Privilege> modelVersionOperationToPrivileges(ModelVersionOperation modelVersionOperation) {
        return switch (modelVersionOperation) {
            case READ_MODEL_VERSION -> Set.of(SELECT);
            case READ_WRITE_MODEL_VERSION -> Set.of(SELECT, UPDATE);
            case UNKNOWN_MODEL_VERSION_OPERATION ->
                    throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Unknown operation in the request: " + ModelVersionOperation.UNKNOWN_MODEL_VERSION_OPERATION);
        };
    }
}
