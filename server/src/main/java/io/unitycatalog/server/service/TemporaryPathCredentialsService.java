package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.KeyMapper;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.GenerateTemporaryPathCredential;
import io.unitycatalog.server.model.PathOperation;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.PathUtils;
import io.unitycatalog.server.utils.PathUtils.PathComponents;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.SneakyThrows;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryPathCredentialsService {
  private final CloudCredentialVendor cloudCredentialVendor;
  private final UserRepository userRepository;
  private final KeyMapper keyMapper;
  private final UnityAccessEvaluator evaluator;

  @SneakyThrows
  public TemporaryPathCredentialsService(
      UnityCatalogAuthorizer authorizer,
      CloudCredentialVendor cloudCredentialVendor,
      Repositories repositories) {
    this.cloudCredentialVendor = cloudCredentialVendor;
    this.userRepository = repositories.getUserRepository();
    this.keyMapper = new KeyMapper(repositories);
    this.evaluator = new UnityAccessEvaluator(authorizer);
  }

  @Post("")
  public HttpResponse generateTemporaryPathCredential(
      GenerateTemporaryPathCredential generateTemporaryPathCredential) {

    authorizeForOperation(generateTemporaryPathCredential);

    return HttpResponse.ofJson(
        cloudCredentialVendor.vendCredential(
            generateTemporaryPathCredential.getUrl(),
            pathOperationToPrivileges(generateTemporaryPathCredential.getOperation())));
  }

  private Set<CredentialContext.Privilege> pathOperationToPrivileges(PathOperation pathOperation) {
    return switch (pathOperation) {
      case PATH_READ -> Set.of(SELECT);
      case PATH_READ_WRITE, PATH_CREATE_TABLE -> Set.of(SELECT, UPDATE);
      case UNKNOWN_PATH_OPERATION -> Collections.emptySet();
    };
  }

  private void authorizeForOperation(
      GenerateTemporaryPathCredential generateTemporaryPathCredential) {
    // Extract catalog and schema from the path
    String requestedPath = generateTemporaryPathCredential.getUrl();
    PathComponents pathComponents = PathUtils.extractPathComponents(requestedPath);

    // Validate that the path belongs to an authorized schema
    validatePathScope(pathComponents, generateTemporaryPathCredential.getOperation());
  }

  private void validatePathScope(PathComponents pathComponents, PathOperation operation) {
    if (pathComponents.catalogName() != null
        && pathComponents.schemaName() != null
        && operation == PathOperation.PATH_CREATE_TABLE) {
      evaluateCreateTableForSchema(pathComponents);
      return;
    }

    if (pathComponents.catalogName() != null && operation == PathOperation.PATH_CREATE_TABLE) {
      evaluateCreateTableForCatalog(pathComponents);
      return;
    }

    evaluateArbitraryPathOperation();
  }

  private void evaluateCreateTableForSchema(PathComponents pathComponents) {
    String authorizeExpression =
        "#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) && #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA)";
    Map<SecurableType, Object> keyMap =
        new HashMap<>(
            Map.of(SCHEMA, pathComponents.schemaName(), CATALOG, pathComponents.catalogName()));
    evaluateResources(keyMap, authorizeExpression);
  }

  private void evaluateCreateTableForCatalog(PathComponents pathComponents) {
    String authorizeExpression = "#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)";
    Map<SecurableType, Object> keyMap =
        new HashMap<>(Map.of(CATALOG, pathComponents.catalogName()));
    evaluateResources(keyMap, authorizeExpression);
  }

  private void evaluateArbitraryPathOperation() {
    String authorizeExpression = "#authorize(#principal, #metastore, OWNER)";
    Map<SecurableType, Object> keyMap = new HashMap<>(Map.of(METASTORE, "metastore"));
    evaluateResources(keyMap, authorizeExpression);
  }

  private void evaluateResources(Map<SecurableType, Object> keyMap, String authorizeExpression) {
    Map<SecurableType, Object> resourceKeys = keyMapper.mapResourceKeys(keyMap);
    if (!evaluator.evaluate(userRepository.findPrincipalId(), authorizeExpression, resourceKeys)) {
      throw new BaseException(ErrorCode.PERMISSION_DENIED, "Access denied");
    }
  }
}
