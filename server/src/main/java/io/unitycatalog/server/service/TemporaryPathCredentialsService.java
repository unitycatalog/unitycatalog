package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.GenerateTemporaryPathCredential;
import io.unitycatalog.server.model.PathOperation;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.CredentialOperations;

import java.util.Collections;
import java.util.Set;

import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryPathCredentialsService {
    private final CredentialOperations credentialOps;

    public TemporaryPathCredentialsService(CredentialOperations credentialOps) {
        this.credentialOps = credentialOps;
    }

    @Post("")
    public HttpResponse generateTemporaryPathCredential(
        GenerateTemporaryPathCredential generateTemporaryPathCredential) {
        return HttpResponse.ofJson(
                credentialOps.vendCredential(
                        generateTemporaryPathCredential.getUrl(),
                        pathOperationToPrivileges(generateTemporaryPathCredential.getOperation())));
    }

    private Set<CredentialContext.Privilege> pathOperationToPrivileges(PathOperation pathOperation) {
        return switch (pathOperation) {
            // Should PATH_REFRESH be SELECT?
            case PATH_READ, PATH_REFRESH -> Set.of(SELECT);
            case PATH_READ_WRITE, PATH_CREATE_TABLE -> Set.of(SELECT, UPDATE);
            case UNKNOWN_PATH_OPERATION -> Collections.emptySet();
        };
    }
}
