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
import io.unitycatalog.server.model.GenerateTemporaryVolumeCredential;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.model.VolumeOperation;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.VolumeRepository;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.CredentialOperations;
import lombok.SneakyThrows;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.VOLUME;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryVolumeCredentialsService {
  private final VolumeRepository volumeRepository;
  private final UserRepository userRepository;

  private final UnityAccessEvaluator evaluator;
  private final CredentialOperations credentialOps;
  private final KeyMapper keyMapper;

  @SneakyThrows
  public TemporaryVolumeCredentialsService(UnityCatalogAuthorizer authorizer, CredentialOperations credentialOps, Repositories repositories) {
    this.evaluator = new UnityAccessEvaluator(authorizer);
    this.credentialOps = credentialOps;
    this.keyMapper = new KeyMapper(repositories);
    this.volumeRepository = repositories.getVolumeRepository();
    this.userRepository = repositories.getUserRepository();
  }

  @Post("")
  public HttpResponse generateTemporaryTableCredential(GenerateTemporaryVolumeCredential generateTemporaryVolumeCredential) {
    authorizeForOperation(generateTemporaryVolumeCredential);

    String volumeId = generateTemporaryVolumeCredential.getVolumeId();
    if (volumeId.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Volume ID is required.");
    }
    VolumeInfo volumeInfo = volumeRepository.getVolumeById(volumeId);
    return HttpResponse.ofJson(
            credentialOps.vendCredential(
                    volumeInfo.getStorageLocation(),
                    volumeOperationToPrivileges(generateTemporaryVolumeCredential.getOperation())));
  }

  private Set<CredentialContext.Privilege> volumeOperationToPrivileges(VolumeOperation volumeOperation) {
    return switch (volumeOperation) {
      case READ_VOLUME -> Set.of(SELECT);
      case WRITE_VOLUME -> Set.of(SELECT, UPDATE);
      case UNKNOWN_VOLUME_OPERATION -> Collections.emptySet();
    };
  }

  private void authorizeForOperation(GenerateTemporaryVolumeCredential generateTemporaryVolumeCredential) {

    // TODO: This is a short term solution to conditional expression evaluation based on additional request parameters.
    // This should be replaced with more direct annotations and syntax in the future.

    String readExpression = """
          #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) && #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) && #authorizeAny(#principal, #volume, OWNER, READ_VOLUME)
          """;

    // TODO: add WRITE_VOLUME to the expression
    String writeExpression = """
          #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
          #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
          #authorize(#principal, #volume, OWNER)
          """;

    String authorizeExpression =
            generateTemporaryVolumeCredential.getOperation() == VolumeOperation.READ_VOLUME ?
                    readExpression : writeExpression;

    Map<SecurableType, Object> resourceKeys = keyMapper.mapResourceKeys(
            Map.of(METASTORE, "metastore",
                    VOLUME, generateTemporaryVolumeCredential.getVolumeId()));

    if (!evaluator.evaluate(userRepository.findPrincipalId(), authorizeExpression, resourceKeys)) {
      throw new BaseException(ErrorCode.PERMISSION_DENIED, "Access denied.");
    }
  }

}
