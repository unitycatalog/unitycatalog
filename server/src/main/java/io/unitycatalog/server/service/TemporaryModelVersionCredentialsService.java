package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.KeyMapper;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.ModelRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.service.credential.CredentialOperations;
import io.unitycatalog.server.service.credential.CredentialContext;
import lombok.SneakyThrows;

import java.util.Map;
import java.util.Set;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.REGISTERED_MODEL;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryModelVersionCredentialsService {
    private final ModelRepository modelRepository;
    private final UserRepository userRepository;

    private final UnityAccessEvaluator evaluator;
    private final CredentialOperations credentialOps;
    private final KeyMapper keyMapper;

    @SneakyThrows
    public TemporaryModelVersionCredentialsService(UnityCatalogAuthorizer authorizer, CredentialOperations credentialOps, Repositories repositories) {
        this.evaluator = new UnityAccessEvaluator(authorizer);
        this.credentialOps = credentialOps;
        this.keyMapper = new KeyMapper(repositories);
        this.modelRepository = repositories.getModelRepository();
        this.userRepository = repositories.getUserRepository();
    }

    @Post("")
    public HttpResponse generateTemporaryModelVersionCredentials(
            GenerateTemporaryModelVersionCredential generateTemporaryModelVersionCredentials) {
        authorizeForOperation(generateTemporaryModelVersionCredentials);

        long modelVersion = generateTemporaryModelVersionCredentials.getVersion();
        String catalogName = generateTemporaryModelVersionCredentials.getCatalogName();
        String schemaName = generateTemporaryModelVersionCredentials.getSchemaName();
        String modelName = generateTemporaryModelVersionCredentials.getModelName();
        String fullName = RepositoryUtils.getAssetFullName(catalogName, schemaName, modelName);

        ModelVersionInfo modelVersionInfo = modelRepository.getModelVersion(fullName, modelVersion);
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
                credentialOps.vendCredential(
                        modelVersionInfo.getStorageLocation(),
                        modelVersionOperationToPrivileges(requestedOperation)));
    }

    private Set<CredentialContext.Privilege> modelVersionOperationToPrivileges(
            ModelVersionOperation modelVersionOperation) {
        return switch (modelVersionOperation) {
            case READ_MODEL_VERSION -> Set.of(SELECT);
            case READ_WRITE_MODEL_VERSION -> Set.of(SELECT, UPDATE);
            case UNKNOWN_MODEL_VERSION_OPERATION ->
                    throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Unknown operation in the request: " + ModelVersionOperation.UNKNOWN_MODEL_VERSION_OPERATION);
        };
    }

    private void authorizeForOperation(
            GenerateTemporaryModelVersionCredential generateTemporaryModelVersionCredentials) {

        // TODO: This is a short term solution to conditional expression evaluation based on additional request parameters.
        // This should be replaced with more direct annotations and syntax in the future.

        String readExpression = """
          #authorizeAny(#principal, #registered_model, OWNER, EXECUTE) &&
          #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
          #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
          """;

        String writeExpression = """
          #authorize(#principal, #registered_model, OWNER) &&
          #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
          #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
          """;

        String authorizeExpression =
                generateTemporaryModelVersionCredentials.getOperation() == ModelVersionOperation.READ_MODEL_VERSION ?
                        readExpression : writeExpression;

        Map<SecurableType, Object> resourceKeys = keyMapper.mapResourceKeys(
                Map.of(METASTORE, "metastore",
                        CATALOG, generateTemporaryModelVersionCredentials.getCatalogName(),
                        SCHEMA, generateTemporaryModelVersionCredentials.getSchemaName(),
                        REGISTERED_MODEL, generateTemporaryModelVersionCredentials.getModelName()));

        if (!evaluator.evaluate(userRepository.findPrincipalId(), authorizeExpression, resourceKeys)) {
            throw new BaseException(ErrorCode.PERMISSION_DENIED, "Access denied.");
        }
    }
}
