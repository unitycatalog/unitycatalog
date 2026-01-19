package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.REGISTERED_MODEL;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;

import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKeys;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.GenerateTemporaryModelVersionCredential;
import io.unitycatalog.server.model.ModelVersionInfo;
import io.unitycatalog.server.model.ModelVersionOperation;
import io.unitycatalog.server.model.ModelVersionStatus;
import io.unitycatalog.server.persist.ModelRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.Set;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryModelVersionCredentialsService {
  private final ModelRepository modelRepository;
  private final CloudCredentialVendor cloudCredentialVendor;

  public TemporaryModelVersionCredentialsService(CloudCredentialVendor cloudCredentialVendor,
                                                 Repositories repositories) {
    this.cloudCredentialVendor = cloudCredentialVendor;
    this.modelRepository = repositories.getModelRepository();
  }

  @Post("")
  @AuthorizeExpression("""
      #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      (#operation == 'READ_MODEL_VERSION'
        ? #authorizeAny(#principal, #registered_model, OWNER, EXECUTE)
        : #authorize(#principal, #registered_model, OWNER))
      """)
  public HttpResponse generateTemporaryModelVersionCredentials(
      @AuthorizeResourceKeys({
        @AuthorizeResourceKey(value = CATALOG, key = "catalog_name"),
        @AuthorizeResourceKey(value = SCHEMA, key = "schema_name"),
        @AuthorizeResourceKey(value = REGISTERED_MODEL, key = "model_name")
      })
      @AuthorizeKey(key = "operation")
      GenerateTemporaryModelVersionCredential generateTemporaryModelVersionCredentials) {

    long modelVersion = generateTemporaryModelVersionCredentials.getVersion();
    String catalogName = generateTemporaryModelVersionCredentials.getCatalogName();
    String schemaName = generateTemporaryModelVersionCredentials.getSchemaName();
    String modelName = generateTemporaryModelVersionCredentials.getModelName();
    String fullName = RepositoryUtils.getAssetFullName(catalogName, schemaName, modelName);

    ModelVersionInfo modelVersionInfo = modelRepository.getModelVersion(fullName, modelVersion);
    String storageLocation = modelVersionInfo.getStorageLocation();
    if (storageLocation.toLowerCase().startsWith("file")) {
      String errorMsg = String.format(
          "Cannot request credentials on a model version with a file based storage location: %s/%d",
          fullName, modelVersion);
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, errorMsg);
    }
    ModelVersionOperation requestedOperation =
        generateTemporaryModelVersionCredentials.getOperation();
    // Must enforce that the status of the model version matches the requested credential type.
    if (modelVersionInfo.getStatus() == ModelVersionStatus.FAILED_REGISTRATION
        || modelVersionInfo.getStatus() == ModelVersionStatus.MODEL_VERSION_STATUS_UNKNOWN) {
      String errorMsg = String.format(
          "Cannot request credentials on a model version with status %s: %s/%d",
          modelVersionInfo.getStatus().getValue(), fullName, modelVersion);
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, errorMsg);
    }
    if ((modelVersionInfo.getStatus() != ModelVersionStatus.PENDING_REGISTRATION
        && requestedOperation == ModelVersionOperation.READ_WRITE_MODEL_VERSION)) {
      String errorMsg = String.format(
          "Cannot request read/write credentials on a model version that has been finalized: %s/%d",
          fullName, modelVersion);
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, errorMsg);
    }
    return HttpResponse.ofJson(
        cloudCredentialVendor.vendCredential(
            NormalizedURL.from(modelVersionInfo.getStorageLocation()),
            modelVersionOperationToPrivileges(requestedOperation)));
  }

  private Set<CredentialContext.Privilege> modelVersionOperationToPrivileges(
      ModelVersionOperation modelVersionOperation) {
    return switch (modelVersionOperation) {
      case READ_MODEL_VERSION -> Set.of(SELECT);
      case READ_WRITE_MODEL_VERSION -> Set.of(SELECT, UPDATE);
      case UNKNOWN_MODEL_VERSION_OPERATION -> throw new BaseException(ErrorCode.INVALID_ARGUMENT,
          "Unknown operation in the request: " + ModelVersionOperation.UNKNOWN_MODEL_VERSION_OPERATION);
    };
  }
}
