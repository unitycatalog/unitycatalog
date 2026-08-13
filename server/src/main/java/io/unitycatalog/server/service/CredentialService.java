package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CREDENTIAL;
import static io.unitycatalog.server.model.SecurableType.METASTORE;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.ResponseAuthorizeFilter;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.ListCredentialsResponse;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.UpdateCredentialRequest;
import io.unitycatalog.server.persist.CredentialRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.utils.ServerProperties;
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

/**
 * This service manages cloud provider credentials used to access cloud storage (S3, ADLS, GCS) for
 * accessing external locations.
 *
 * <h2>Supported Cloud Providers</h2>
 *
 * <ul>
 *   <li><b>AWS</b> - IAM Role-based credentials
 *   <li><b>Azure</b> - Not implemented yet.
 *   <li><b>GCP</b> - Not implemented yet.
 * </ul>
 */
@ExceptionHandler(GlobalExceptionHandler.class)
public class CredentialService extends AuthorizedService {
  private final CredentialRepository credentialRepository;
  private final MetastoreRepository metastoreRepository;

  @SneakyThrows
  public CredentialService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties) {
    super(authorizer, repositories, serverProperties);
    this.credentialRepository = repositories.getCredentialRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
  }

  @Post("")
  // NOTE: service credential and CREATE_SERVICE_CREDENTIAL are not supported.
  @AuthorizeExpression("#authorizeAny(#principal, #metastore, OWNER, CREATE_STORAGE_CREDENTIAL)")
  @AuthorizeResourceKey(METASTORE)
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
  @AuthorizeExpression(LIST_AND_GET_AUTH_EXPRESSION)
  @ResponseAuthorizeFilter
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse listCredentials(
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {
    ListCredentialsResponse credentials =
        credentialRepository.listCredentials(maxResults, pageToken);
    applyResponseFilter(SecurableType.CREDENTIAL, credentials.getCredentials());
    return HttpResponse.ofJson(credentials);
  }

  @Get("/{name}")
  @AuthorizeExpression(LIST_AND_GET_AUTH_EXPRESSION)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse getCredential(@Param("name") @AuthorizeResourceKey(CREDENTIAL) String name) {
    return HttpResponse.ofJson(credentialRepository.getCredential(name));
  }

  @Patch("/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #credential, OWNER)
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateCredential(
      @Param("name") @AuthorizeResourceKey(CREDENTIAL) String name,
      UpdateCredentialRequest updateRequest) {
    return HttpResponse.ofJson(credentialRepository.updateCredential(name, updateRequest));
  }

  @Delete("/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #credential, OWNER)
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse deleteCredential(
      @Param("name") @AuthorizeResourceKey(CREDENTIAL) String name,
      @Param("force") Optional<Boolean> force) {
    UUID deletedCredentialId =
        credentialRepository.deleteCredential(name, force.orElse(false));
    removeAuthorizations(deletedCredentialId.toString());
    return HttpResponse.of(HttpStatus.OK);
  }

}
