package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Patch;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CreateCatalog;
import io.unitycatalog.server.model.ListCatalogsResponse;
import io.unitycatalog.server.model.UpdateCatalog;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.Privileges;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class CatalogService {
  private final CatalogRepository catalogRepository;
  private final MetastoreRepository metastoreRepository;
  private final UserRepository userRepository;
  private final UnityCatalogAuthorizer authorizer;
  private final UnityAccessEvaluator evaluator;

  @SneakyThrows
  public CatalogService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    this.authorizer = authorizer;
    this.evaluator = new UnityAccessEvaluator(authorizer);
    this.catalogRepository = repositories.getCatalogRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
    this.userRepository = repositories.getUserRepository();
  }

  @Post("")
  @AuthorizeExpression("#authorizeAny(#principal, #metastore, OWNER, CREATE_CATALOG)")
  @AuthorizeKey(METASTORE)
  public HttpResponse createCatalog(CreateCatalog createCatalog) {
    CatalogInfo catalogInfo = catalogRepository.addCatalog(createCatalog);
    initializeAuthorizations(catalogInfo);
    return HttpResponse.ofJson(catalogInfo);
  }

  @Get("")
  @AuthorizeExpression("#defer")
  public HttpResponse listCatalogs(
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {
    ListCatalogsResponse listCatalogsResponse =
        catalogRepository.listCatalogs(maxResults, pageToken);

    filterCatalogs("""
        #authorize(#principal, #metastore, OWNER) ||
        #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
        """,
        listCatalogsResponse.getCatalogs());

    return HttpResponse.ofJson(listCatalogsResponse);
  }

  @Get("/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse getCatalog(@Param("name") @AuthorizeKey(CATALOG) String name) {
    return HttpResponse.ofJson(catalogRepository.getCatalog(name));
  }

  @Patch("/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #catalog, OWNER)
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse updateCatalog(
      @Param("name") @AuthorizeKey(CATALOG) String name, UpdateCatalog updateCatalog) {
    return HttpResponse.ofJson(catalogRepository.updateCatalog(name, updateCatalog));
  }

  @Delete("/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse deleteCatalog(
      @Param("name") @AuthorizeKey(CATALOG) String name, @Param("force") Optional<Boolean> force) {
    CatalogInfo catalogInfo = catalogRepository.getCatalog(name);
    catalogRepository.deleteCatalog(name, force.orElse(false));
    removeAuthorizations(catalogInfo);
    return HttpResponse.of(HttpStatus.OK);
  }

  public void filterCatalogs(String expression, List<CatalogInfo> entries) {
    // TODO: would be nice to move this to filtering in the Decorator response
    UUID principalId = userRepository.findPrincipalId();

    evaluator.filter(
        principalId,
        expression,
        entries,
        ci ->
            Map.of(
                METASTORE,
                metastoreRepository.getMetastoreId(),
                CATALOG,
                UUID.fromString(ci.getId())));
  }

  private void initializeAuthorizations(CatalogInfo catalogInfo) {
    UUID principalId = userRepository.findPrincipalId();
    authorizer.grantAuthorization(
        principalId, UUID.fromString(catalogInfo.getId()), Privileges.OWNER);
  }

  private void removeAuthorizations(CatalogInfo catalogInfo) {
    authorizer.clearAuthorizationsForResource(UUID.fromString(catalogInfo.getId()));
  }
}
