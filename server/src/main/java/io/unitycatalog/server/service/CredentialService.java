package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.ListCredentialsResponse;
import io.unitycatalog.server.model.UpdateCredentialRequest;
import io.unitycatalog.server.persist.CredentialRepository;
import io.unitycatalog.server.persist.Repositories;
import lombok.SneakyThrows;

import java.util.Optional;

import static io.unitycatalog.server.model.SecurableType.METASTORE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class CredentialService extends AuthorizedService {
  private final CredentialRepository credentialRepository;

  @SneakyThrows
  public CredentialService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    super(authorizer, repositories.getUserRepository());
    this.credentialRepository = repositories.getCredentialRepository();
  }

  @Post("")
  // TODO: Add CREATE_CREDENTIAL privilege
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeKey(METASTORE)
  public HttpResponse createCredential(CreateCredentialRequest createCredentialRequest) {
    CredentialInfo credentialInfo = credentialRepository.addCredential(createCredentialRequest);
    initializeBasicAuthorization(credentialInfo.getId());
    return HttpResponse.ofJson(credentialInfo);
  }

  @Get("")
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  public HttpResponse listCredentials(
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {
    ListCredentialsResponse credentials =
        credentialRepository.listCredentials(maxResults, pageToken);
    return HttpResponse.ofJson(credentials);
  }

  @Get("/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER)
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse getCredential(@Param("name") String name) {
    return HttpResponse.ofJson(credentialRepository.getCredential(name));
  }

  @Patch("/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER)
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse updateCredential(
      @Param("name") String name, UpdateCredentialRequest updateRequest) {
    return HttpResponse.ofJson(credentialRepository.updateCredential(name, updateRequest));
  }

  @Delete("/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER)
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse deleteCredential(@Param("name") String name) {
    CredentialInfo credentialInfo = credentialRepository.deleteCredential(name);
    removeAuthorizations(credentialInfo.getId());
    return HttpResponse.of(HttpStatus.OK);
  }
}
