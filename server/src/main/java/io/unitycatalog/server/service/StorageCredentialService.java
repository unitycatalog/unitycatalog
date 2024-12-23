package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateStorageCredential;
import io.unitycatalog.server.model.ListStorageCredentialsResponse;
import io.unitycatalog.server.model.StorageCredentialInfo;
import io.unitycatalog.server.model.UpdateStorageCredential;
import io.unitycatalog.server.persist.StorageCredentialRepository;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.IdentityUtils;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static io.unitycatalog.server.model.SecurableType.METASTORE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class StorageCredentialService {
    private static final StorageCredentialRepository REPOSITORY = StorageCredentialRepository.getInstance();
    private final UnityCatalogAuthorizer authorizer;
    private final UnityAccessEvaluator evaluator;

    @SneakyThrows
    public StorageCredentialService(UnityCatalogAuthorizer authorizer) {
        this.authorizer = authorizer;
        evaluator = new UnityAccessEvaluator(authorizer);
    }

    @Post("")
    // TODO: Add CREATE_STORAGE_CREDENTIAL privilege?
    @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
    @AuthorizeKey(METASTORE)
    public HttpResponse createStorageCredential(CreateStorageCredential createStorageCredential) {
        StorageCredentialInfo storageCredentialInfo = REPOSITORY.addStorageCredential(createStorageCredential);
        initializeAuthorizations(storageCredentialInfo);
        return HttpResponse.ofJson(storageCredentialInfo);
    }

    @Get("")
    @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
    public HttpResponse listStorageCredentials(
            @Param("max_results") Optional<Integer> maxResults,
            @Param("page_token") Optional<String> pageToken) {
        ListStorageCredentialsResponse credentials = REPOSITORY.listStorageCredentials(maxResults, pageToken);
        return HttpResponse.ofJson(credentials);
    }

    @Get("/{name}")
    @AuthorizeExpression("""
            #authorize(#principal, #metastore, OWNER)
            """)
    @AuthorizeKey(METASTORE)
    public HttpResponse getStorageCredential(@Param("name") String name) {
        return HttpResponse.ofJson(REPOSITORY.getStorageCredential(name));
    }

    @Patch("/{name}")
    @AuthorizeExpression("""
            #authorize(#principal, #storageCredential, OWNER)
            """)
    @AuthorizeKey(METASTORE)
    public HttpResponse updateStorageCredential(
            @Param("name") String name, UpdateStorageCredential updateRequest) {
        return HttpResponse.ofJson(REPOSITORY.updateStorageCredential(name, updateRequest));
    }

    @Delete("/{name}")
    @AuthorizeExpression("""
            #authorize(#principal, #metastore, OWNER)
            """)
    @AuthorizeKey(METASTORE)
    public HttpResponse deleteStorageCredential(@Param("name") String name) {
        StorageCredentialInfo storageCredentialInfo = REPOSITORY.deleteStorageCredential(name);
        removeAuthorizations(storageCredentialInfo);
        return HttpResponse.of(HttpStatus.OK);
    }

    private void initializeAuthorizations(StorageCredentialInfo storageCredentialInfo) {
        UUID principalId = IdentityUtils.findPrincipalId();
        authorizer.grantAuthorization(
                principalId, UUID.fromString(storageCredentialInfo.getId()), Privileges.OWNER);
    }

    private void removeAuthorizations(StorageCredentialInfo storageCredentialInfo) {
        authorizer.clearAuthorizationsForResource(UUID.fromString(storageCredentialInfo.getId()));
    }
}
