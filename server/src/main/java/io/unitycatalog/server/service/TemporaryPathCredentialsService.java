package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.GenerateTemporaryPathCredential;
import io.unitycatalog.server.model.PathOperation;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryPathCredentialsService extends AuthorizedService {
  private final CloudCredentialVendor cloudCredentialVendor;

  public TemporaryPathCredentialsService(
      UnityCatalogAuthorizer authorizer,
      CloudCredentialVendor cloudCredentialVendor,
      Repositories repositories) {
    super(authorizer, repositories);
    this.cloudCredentialVendor = cloudCredentialVendor;
  }

  @Post("")
  @AuthorizeExpression("#defer")
  public HttpResponse generateTemporaryPathCredential(
      GenerateTemporaryPathCredential generateTemporaryPathCredential) {
    authorizeForOperation(generateTemporaryPathCredential);

    return HttpResponse.ofJson(
        cloudCredentialVendor.vendCredential(
            NormalizedURL.from(generateTemporaryPathCredential.getUrl()),
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
    // The authorization will be in this order:
    // 1. if user is owner of metastore, always allow
    // 2. if the path is a parent of any data securable or external location, fail with error.
    // 3. if the path is a part of more than one data securable or more than one external location,
    //  fail with error.
    // 4. if the path is found to be part of one data securable:
    //   4a. if the path is found to belong to a table, user needs and only needs to have access to
    //    the table.
    //   4b. ditto for volume and registered_model
    // 5. Otherwise, the path must belong to one external location and user must have access to
    //  it.
    // This function will only set resource key for ONE of the table, volume, model, or external
    // location.

    final String readExpression = """
        #authorize(#principal, #metastore, OWNER) ||
        (#table != null &&
         #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
         #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
         #authorizeAny(#principal, #table, OWNER, SELECT)) ||
        (#volume != null &&
         #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
         #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
         #authorizeAny(#principal, #volume, OWNER, READ_VOLUME)) ||
        (#registered_model != null &&
         #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
         #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
         #authorizeAny(#principal, #registered_model, OWNER, EXECUTE)) ||
        (#external_location != null &&
         #authorizeAny(#principal, #external_location, OWNER, READ_FILES))
        """;

    final String readWriteExpression = """
        #authorize(#principal, #metastore, OWNER) ||
        (#table != null &&
         #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
         #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
         (#authorize(#principal, #table, OWNER) ||
          #authorizeAll(#principal, #table, SELECT, MODIFY))) ||
        (#volume != null &&
         #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
         #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
         #authorize(#principal, #volume, OWNER)) ||
        (#registered_model != null &&
         #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
         #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
         #authorize(#principal, #registered_model, OWNER)) ||
        (#external_location != null &&
         (#authorize(#principal, #external_location, OWNER) ||
          #authorizeAll(#principal, #external_location, READ_FILES, WRITE_FILES)))
        """;

    // The path must not match any of the existing objects other than external location
    final String createExternalTableExpression = """
        #volume == null &&
        #table == null &&
        #registered_model == null &&
        (#authorize(#principal, #metastore, OWNER) ||
         (#external_location != null &&
          #authorizeAny(#principal, #external_location, OWNER, CREATE_EXTERNAL_TABLE)))
        """;

    String authorizeExpression = switch (generateTemporaryPathCredential.getOperation()) {
      case PATH_READ -> readExpression;
      case PATH_READ_WRITE -> readWriteExpression;
      case PATH_CREATE_TABLE -> createExternalTableExpression;
      default -> throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid operation: " + generateTemporaryPathCredential.getOperation());
    };

    Map<SecurableType, Object> resourceKeys =
        keyMapper.mapResourceKeys(
            Map.of(
                SecurableType.METASTORE,
                "metastore",
                // EXTERNAL_LOCATION is just a placeholder. The actual resources will be resolved.
                SecurableType.EXTERNAL_LOCATION,
                generateTemporaryPathCredential.getUrl()));
    if (!evaluator.evaluate(userRepository.findPrincipalId(), authorizeExpression, resourceKeys)) {
      throw new BaseException(ErrorCode.PERMISSION_DENIED, "Access denied.");
    }
  }
}
