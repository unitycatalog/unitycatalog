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
    public HttpResponse generateTemporaryModelVersionCredential(
            GenerateTemporaryModelVersionCredential generateTemporaryModelVersionCredential) {

        long modelVersion = generateTemporaryModelVersionCredential.getVersion();
        String catalogName = generateTemporaryModelVersionCredential.getCatalogName();
        String schemaName = generateTemporaryModelVersionCredential.getSchemaName();
        String modelName = generateTemporaryModelVersionCredential.getModelName();
        String fullName = RepositoryUtils.getAssetFullName(catalogName, schemaName, modelName);

        ModelVersionInfo modelVersionInfo = MODEL_REPOSITORY.getModelVersion(fullName, modelVersion);
        ModelVersionOperation requestedOperation = generateTemporaryModelVersionCredential.getOperation();
        // Must enforce that write credentials are not passed back if the model has been finalized
        if (modelVersionInfo.getStatus() != ModelVersionStatus.PENDING_REGISTRATION && requestedOperation == ModelVersionOperation.READ_WRITE_MODEL_VERSION) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Cannot request read/write credentials on a finalized model version: " + fullName + "/" + modelVersion);
        }
        return HttpResponse.ofJson(credentialOps.vendCredentialForModelVersion(modelVersionInfo, modelVersionOperationToPrivileges(generateTemporaryModelVersionCredential.getOperation())));
    }

    private Set<CredentialContext.Privilege> modelVersionOperationToPrivileges(ModelVersionOperation modelVersionOperation) {
        return switch (modelVersionOperation) {
            case READ_MODEL_VERSION -> Set.of(SELECT);
            case READ_WRITE_MODEL_VERSION -> Set.of(SELECT, UPDATE);
            case UNKNOWN_MODEL_VERSION_OPERATION -> Collections.emptySet();
        };
    }
}
