package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.decorator.KeyMapperUtil;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.GenerateTemporaryModelVersionCredential;
import io.unitycatalog.server.model.GenerateTemporaryTableCredential;
import io.unitycatalog.server.model.ModelVersionOperation;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableOperation;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.CredentialOperations;
import io.unitycatalog.server.utils.IdentityUtils;
import lombok.SneakyThrows;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.REGISTERED_MODEL;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryTableCredentialsService {

  private static final TableRepository TABLE_REPOSITORY = TableRepository.getInstance();

  private final UnityAccessEvaluator evaluator;
  private final CredentialOperations credentialOps;

  @SneakyThrows
  public TemporaryTableCredentialsService(UnityCatalogAuthorizer authorizer, CredentialOperations credentialOps) {
    this.evaluator = new UnityAccessEvaluator(authorizer);
    this.credentialOps = credentialOps;
  }

  @Post("")
  public HttpResponse generateTemporaryTableCredential(GenerateTemporaryTableCredential generateTemporaryTableCredential) {
    authorizeForOperation(generateTemporaryTableCredential);

    String tableId = generateTemporaryTableCredential.getTableId();
    TableInfo tableInfo = TABLE_REPOSITORY.getTableById(tableId);
    return HttpResponse.ofJson(credentialOps
            .vendCredential(tableInfo.getStorageLocation(),
                    tableOperationToPrivileges(generateTemporaryTableCredential.getOperation())));
  }

  private Set<CredentialContext.Privilege> tableOperationToPrivileges(TableOperation tableOperation) {
    return switch (tableOperation) {
      case READ -> Set.of(SELECT);
      case READ_WRITE -> Set.of(SELECT, UPDATE);
      case UNKNOWN_TABLE_OPERATION -> Collections.emptySet();
    };
  }

  private void authorizeForOperation(GenerateTemporaryTableCredential generateTemporaryTableCredential) {

    // TODO: This is a short term solution to conditional expression evaluation based on additional request parameters.
    // This should be replaced with more direct annotations and syntax in the future.

    String readExpression = """
          #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) && #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) && #authorizeAny(#principal, #table, OWNER, SELECT)
          """;

    // TODO: add MODIFY to the expression
    String writeExpression = """
          #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) && #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) && #authorize(#principal, #table, OWNER)
          """;

    String authorizeExpression =
            generateTemporaryTableCredential.getOperation() ==  TableOperation.READ ?
                    readExpression : writeExpression;

    Map<SecurableType, Object> resourceKeys = KeyMapperUtil.mapResourceKeys(
            Map.of(METASTORE, "metastore",
                    TABLE, generateTemporaryTableCredential.getTableId()));

    if (!evaluator.evaluate(IdentityUtils.findPrincipalId(), authorizeExpression, resourceKeys)) {
      throw new BaseException(ErrorCode.PERMISSION_DENIED, "Access denied.");
    }
  }

}
