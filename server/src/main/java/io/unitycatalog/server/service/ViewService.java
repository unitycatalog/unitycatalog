package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeKeys;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CreateView;
import io.unitycatalog.server.model.ListViewsResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.ViewInfo;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.ViewRepository;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.VIEW;

@ExceptionHandler(GlobalExceptionHandler.class)
public class ViewService extends AuthorizedService {

  private final ViewRepository viewRepository;
  private final SchemaRepository schemaRepository;
  private final CatalogRepository catalogRepository;
  private final MetastoreRepository metastoreRepository;

  @SneakyThrows
  public ViewService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    super(authorizer, repositories.getUserRepository());
    this.viewRepository = repositories.getViewRepository();
    this.schemaRepository = repositories.getSchemaRepository();
    this.catalogRepository = repositories.getCatalogRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
  }

  @Post("")
  @AuthorizeExpression("""
          (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
              #authorize(#principal, #schema, OWNER)) ||
          (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
              #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_VIEW))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse createView(
          @AuthorizeKeys({
            @AuthorizeKey(value = SCHEMA, key = "schema_name"),
            @AuthorizeKey(value = CATALOG, key = "catalog_name")
          })
          CreateView createView) {
    assert createView != null;
    ViewInfo viewInfo = viewRepository.createView(createView);

    SchemaInfo schemaInfo =
        schemaRepository.getSchema(viewInfo.getCatalogName() + "." + viewInfo.getSchemaName());
    initializeHierarchicalAuthorization(viewInfo.getViewId(), schemaInfo.getSchemaId());

    return HttpResponse.ofJson(viewInfo);
  }

  @Get("/{full_name}")
  @AuthorizeExpression("""
          #authorize(#principal, #metastore, OWNER) ||
          #authorize(#principal, #catalog, OWNER) ||
          (#authorize(#principal, #schema, OWNER) &&
              #authorize(#principal, #catalog, USE_CATALOG)) ||
          (#authorize(#principal, #schema, USE_SCHEMA) &&
              #authorize(#principal, #catalog, USE_CATALOG) &&
              #authorizeAny(#principal, #view, OWNER, SELECT, MODIFY))
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse getView(@Param("full_name") @AuthorizeKey(VIEW) String fullName) {
    assert fullName != null;
    ViewInfo viewInfo = viewRepository.getView(fullName);
    return HttpResponse.ofJson(viewInfo);
  }

  @Get("")
  @AuthorizeExpression("#defer")
  public HttpResponse listViews(
      @Param("catalog_name") String catalogName,
      @Param("schema_name") String schemaName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken,
      @Param("omit_properties") Optional<Boolean> omitProperties,
      @Param("omit_columns") Optional<Boolean> omitColumns) {

    ListViewsResponse listViewsResponse = viewRepository.listViews(
            catalogName,
            schemaName,
            maxResults,
            pageToken,
            omitProperties.orElse(false),
            omitColumns.orElse(false));

    filterViews("""
          #authorize(#principal, #metastore, OWNER) ||
          #authorize(#principal, #catalog, OWNER) ||
          (#authorize(#principal, #schema, OWNER) &&
              #authorize(#principal, #catalog, USE_CATALOG)) ||
          (#authorize(#principal, #schema, USE_SCHEMA) &&
              #authorize(#principal, #catalog, USE_CATALOG) &&
              #authorizeAny(#principal, #view, OWNER, SELECT, MODIFY))
          """, listViewsResponse.getViews());

    return HttpResponse.ofJson(listViewsResponse);
  }

  @Delete("/{full_name}")
  @AuthorizeExpression("""
          #authorize(#principal, #catalog, OWNER) ||
          (#authorize(#principal, #schema, OWNER) &&
              #authorize(#principal, #catalog, USE_CATALOG)) ||
          (#authorize(#principal, #schema, USE_SCHEMA) &&
              #authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #view, OWNER))
          """)
  public HttpResponse deleteView(@Param("full_name") @AuthorizeKey(VIEW) String fullName) {
    ViewInfo viewInfo = viewRepository.getView(fullName);
    viewRepository.deleteView(fullName);

    SchemaInfo schemaInfo =
        schemaRepository.getSchema(viewInfo.getCatalogName() + "." + viewInfo.getSchemaName());
    removeHierarchicalAuthorizations(viewInfo.getViewId(), schemaInfo.getSchemaId());

    return HttpResponse.of(HttpStatus.OK);
  }

  public void filterViews(String expression, List<ViewInfo> entries) {
    // TODO: would be nice to move this to filtering in the Decorator response
    UUID principalId = userRepository.findPrincipalId();

    evaluator.filter(
            principalId,
            expression,
            entries,
            ti -> {
              CatalogInfo catalogInfo = catalogRepository.getCatalog(ti.getCatalogName());
              SchemaInfo schemaInfo =
                      schemaRepository.getSchema(ti.getCatalogName() + "." + ti.getSchemaName());
              return Map.of(
                      METASTORE,
                      metastoreRepository.getMetastoreId(),
                      CATALOG,
                      UUID.fromString(catalogInfo.getId()),
                      SCHEMA,
                      UUID.fromString(schemaInfo.getSchemaId()),
                      VIEW,
                      UUID.fromString(ti.getViewId()));
            });
  }
}
