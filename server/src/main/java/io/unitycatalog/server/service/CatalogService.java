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
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CreateCatalog;
import io.unitycatalog.server.model.ListCatalogsResponse;
import io.unitycatalog.server.model.UpdateCatalog;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.EXTERNAL_LOCATION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class CatalogService extends AuthorizedService {
  private final CatalogRepository catalogRepository;
  private final MetastoreRepository metastoreRepository;

  @SneakyThrows
  public CatalogService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    super(authorizer, repositories);
    this.catalogRepository = repositories.getCatalogRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
  }

  /**
   * Creating a catalog requires one of OWNER or CREATE_CATALOG permission on metastore.
   * Additionally, if a {@code storage_root} is specified:
   *
   * <ul>
   *   <li>The path has to map to an external_location, not anything else (tables, volumes, models),
   *       to make sure the path isn't under any existing data securables.
   *   <li>User needs to have OWNER or CREATE_MANAGED_STORAGE permission on the external location.
   * </ul>
   *
   * {@code storage_root} is annotated as both a {@link AuthorizeResourceKey} (which maps to owning
   * resource) and {@link AuthorizeKey} (which is simply the raw value of storage_root). This is
   * done so that the expression can check both: 1. if parameter is specified, and 2. which external
   * location (rather than tables etc.) owns the path.
   */
  @Post("")
  @AuthorizeExpression("""
      #authorizeAny(#principal, #metastore, OWNER, CREATE_CATALOG) &&
      (#storage_root == null ||
       (#no_overlap_with_data_securable &&
        #external_location != null &&
        #authorizeAny(#principal, #external_location, OWNER, CREATE_MANAGED_STORAGE)))
    """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse createCatalog(
      @AuthorizeResourceKey(value = EXTERNAL_LOCATION, key = "storage_root")
      @AuthorizeKey(key = "storage_root")
      CreateCatalog createCatalog) {
    CatalogInfo catalogInfo = catalogRepository.addCatalog(createCatalog);
    initializeBasicAuthorization(catalogInfo.getId());
    return HttpResponse.ofJson(catalogInfo);
  }

  @Get("")
  @AuthorizeExpression("#defer")
  public HttpResponse listCatalogs(
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {
    ListCatalogsResponse listCatalogsResponse = catalogRepository.listCatalogs(
        maxResults, pageToken);

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
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse getCatalog(@Param("name") @AuthorizeResourceKey(CATALOG) String name) {
    return HttpResponse.ofJson(catalogRepository.getCatalog(name));
  }

  @Patch("/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #catalog, OWNER)
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateCatalog(
      @Param("name") @AuthorizeResourceKey(CATALOG) String name,
      UpdateCatalog updateCatalog) {
    return HttpResponse.ofJson(catalogRepository.updateCatalog(name, updateCatalog));
  }

  @Delete("/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse deleteCatalog(
      @Param("name") @AuthorizeResourceKey(CATALOG) String name,
      @Param("force") Optional<Boolean> force) {
    CatalogInfo catalogInfo = catalogRepository.getCatalog(name);
    catalogRepository.deleteCatalog(name, force.orElse(false));
    removeAuthorizations(catalogInfo.getId());
    return HttpResponse.of(HttpStatus.OK);
  }

  public void filterCatalogs(String expression, List<CatalogInfo> entries) {
    // TODO: would be nice to move this to filtering in the Decorator response
    UUID principalId = userRepository.findPrincipalId();

    evaluator.filter(
        principalId,
        expression,
        entries,
        ci -> Map.of(
            METASTORE,
            metastoreRepository.getMetastoreId(),
            CATALOG,
            UUID.fromString(ci.getId())));
  }
}
