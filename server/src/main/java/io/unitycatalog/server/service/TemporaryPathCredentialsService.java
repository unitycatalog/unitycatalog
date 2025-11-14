package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.EXTERNAL_LOCATION;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.GenerateTemporaryPathCredential;
import io.unitycatalog.server.model.PathOperation;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import java.util.Collections;
import java.util.Set;
import lombok.SneakyThrows;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryPathCredentialsService {
  private final CloudCredentialVendor cloudCredentialVendor;

  @SneakyThrows
  public TemporaryPathCredentialsService(CloudCredentialVendor cloudCredentialVendor) {
    this.cloudCredentialVendor = cloudCredentialVendor;
  }

  @Post("")
  @AuthorizeExpression("#authorizeAny(#principal, #external_location, OWNER, EXTERNAL_USE_LOCATION)")
  public HttpResponse generateTemporaryPathCredential(
      @AuthorizeKey(value = EXTERNAL_LOCATION, key = "url")
          GenerateTemporaryPathCredential generateTemporaryPathCredential) {

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
}
