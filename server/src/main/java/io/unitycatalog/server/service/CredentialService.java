package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CREDENTIAL;
import static io.unitycatalog.server.model.SecurableType.METASTORE;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.ListCredentialsResponse;
import io.unitycatalog.server.model.UpdateCredentialRequest;
import io.unitycatalog.server.persist.CredentialRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.Patch;
import lombok.SneakyThrows;

@ExceptionHandler(GlobalExceptionHandler.class)
public class CredentialService extends AuthorizedService {
  private final CredentialRepository credentialRepository;
  private final MetastoreRepository metastoreRepository;

  @SneakyThrows
  public CredentialService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    super(authorizer, repositories);
    this.credentialRepository = repositories.getCredentialRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
  }

  @Post("")
  // NOTE: service credential and CREATE_SERVICE_CREDENTIAL are not supported.
  @AuthorizeExpression("#authorizeAny(#principal, #metastore, OWNER, CREATE_STORAGE_CREDENTIAL)")
  @AuthorizeKey(METASTORE)
  public HttpResponse createCredential(CreateCredentialRequest createCredentialRequest) {
    CredentialInfo credentialInfo = credentialRepository.addCredential(createCredentialRequest);
    initializeBasicAuthorization(credentialInfo.getId());
    return HttpResponse.ofJson(credentialInfo);
  }

  private static final String LIST_AND_GET_AUTH_EXPRESSION = """
      #authorize(#principal, #metastore, OWNER) ||
      #authorizeAny(#principal, #credential, OWNER, CREATE_EXTERNAL_LOCATION)
      """;

  @Get("")
  @AuthorizeExpression("#defer")
  public HttpResponse listCredentials(
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {
    ListCredentialsResponse credentials =
        credentialRepository.listCredentials(maxResults, pageToken);
    filterCredentials(LIST_AND_GET_AUTH_EXPRESSION, credentials.getCredentials());
    return HttpResponse.ofJson(credentials);
  }

  @Get("/{name}")
  @AuthorizeExpression(LIST_AND_GET_AUTH_EXPRESSION)
  @AuthorizeKey(METASTORE)
  public HttpResponse getCredential(@Param("name") @AuthorizeKey(CREDENTIAL) String name) {
    return HttpResponse.ofJson(credentialRepository.getCredential(name));
  }

  @Patch("/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #credential, OWNER)
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse updateCredential(
      @Param("name") @AuthorizeKey(CREDENTIAL) String name,
      UpdateCredentialRequest updateRequest) {
    return HttpResponse.ofJson(credentialRepository.updateCredential(name, updateRequest));
  }

  @Delete("/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #credential, OWNER)
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse deleteCredential(
      @Param("name") @AuthorizeKey(CREDENTIAL) String name,
      @Param("force") Optional<Boolean> force) {
    CredentialInfo credentialInfo =
        credentialRepository.deleteCredential(name, force.orElse(false));
    removeAuthorizations(credentialInfo.getId());
    return HttpResponse.of(HttpStatus.OK);
  }

  /**
   * Filters a list of credentials based on the authorization expression.
   *
   * <p>This method removes credentials from the list that the current principal does not
   * have permission to access according to the provided authorization expression. The filtering is
   * done in-place by removing unauthorized entries from the list.
   *
   * @param expression The authorization expression to evaluate (e.g., checking for OWNER or
   *     CREATE_EXTERNAL_LOCATION permissions)
   * @param entries The list of credential entries to filter (modified in-place)
   */
  public void filterCredentials(String expression, List<CredentialInfo> entries) {
    // TODO: would be nice to move this to filtering in the Decorator response
    UUID principalId = userRepository.findPrincipalId();

    evaluator.filter(
        principalId,
        expression,
        entries,
        credentialInfo -> Map.of(
            METASTORE,
            metastoreRepository.getMetastoreId(),
            CREDENTIAL,
            UUID.fromString(credentialInfo.getId())));
  }
}
