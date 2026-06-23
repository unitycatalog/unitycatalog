package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.VIEW;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKeys;
import io.unitycatalog.server.auth.annotation.ResponseAuthorizeFilter;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateView;
import io.unitycatalog.server.model.ListViewsResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.ViewInfo;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.ViewRepository;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Optional;
import lombok.SneakyThrows;

@ExceptionHandler(GlobalExceptionHandler.class)
public class ViewService extends AuthorizedService {

  private final ViewRepository viewRepository;
  private final SchemaRepository schemaRepository;

  @SneakyThrows
  public ViewService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties) {
    super(authorizer, repositories, serverProperties);
    this.viewRepository = repositories.getViewRepository();
    this.schemaRepository = repositories.getSchemaRepository();
  }

  @Post("")
  @AuthorizeExpression(
      """
      (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
          #authorize(#principal, #schema, OWNER)) ||
      (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
          #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_VIEW))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse createView(
      @AuthorizeResourceKeys({
        @AuthorizeResourceKey(value = SCHEMA, key = "schema_name"),
        @AuthorizeResourceKey(value = CATALOG, key = "catalog_name")
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
  @AuthorizeExpression(
      """
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) &&
          #authorize(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #catalog, USE_CATALOG) &&
          #authorizeAny(#principal, #view, OWNER, SELECT, MODIFY))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse getView(
      @Param("full_name") @AuthorizeResourceKey(VIEW) String fullName) {
    assert fullName != null;
    ViewInfo viewInfo = viewRepository.getView(fullName);
    return HttpResponse.ofJson(viewInfo);
  }

  @Get("")
  @AuthorizeExpression(
      """
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) &&
          #authorize(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #catalog, USE_CATALOG) &&
          #authorizeAny(#principal, #view, OWNER, SELECT, MODIFY))
      """)
  @ResponseAuthorizeFilter
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse listViews(
      @Param("catalog_name") @AuthorizeResourceKey(CATALOG) String catalogName,
      @Param("schema_name") @AuthorizeResourceKey(SCHEMA) String schemaName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken,
      @Param("omit_properties") Optional<Boolean> omitProperties,
      @Param("omit_columns") Optional<Boolean> omitColumns) {

    ListViewsResponse listViewsResponse =
        viewRepository.listViews(
            catalogName,
            schemaName,
            maxResults,
            pageToken,
            omitProperties.orElse(false),
            omitColumns.orElse(false));

    applyResponseFilter(SecurableType.VIEW, listViewsResponse.getViews());
    return HttpResponse.ofJson(listViewsResponse);
  }

  @Delete("/{full_name}")
  @AuthorizeExpression(
      """
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) &&
          #authorize(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #view, OWNER))
      """)
  public HttpResponse deleteView(
      @Param("full_name") @AuthorizeResourceKey(VIEW) String fullName) {
    ViewInfo viewInfo = viewRepository.getView(fullName);
    viewRepository.deleteView(fullName);

    SchemaInfo schemaInfo =
        schemaRepository.getSchema(viewInfo.getCatalogName() + "." + viewInfo.getSchemaName());
    removeHierarchicalAuthorizations(viewInfo.getViewId(), schemaInfo.getSchemaId());

    return HttpResponse.of(HttpStatus.OK);
  }
}
