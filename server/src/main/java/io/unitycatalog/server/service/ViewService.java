package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;

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
import io.unitycatalog.server.model.CreateView;
import io.unitycatalog.server.model.ListViewsResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.ViewInfo;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.ViewRepository;
import lombok.SneakyThrows;

@ExceptionHandler(GlobalExceptionHandler.class)
public class ViewService extends AuthorizedService {

  private final ViewRepository viewRepository;
  private final SchemaRepository schemaRepository;

  @SneakyThrows
  public ViewService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    super(authorizer, repositories.getUserRepository());
    this.viewRepository = repositories.getViewRepository();
    this.schemaRepository = repositories.getSchemaRepository();
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
      (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #catalog, USE_CATALOG) &&
          #authorizeAny(#principal, #table, OWNER, SELECT, MODIFY))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse getView(@Param("full_name") @AuthorizeKey(TABLE) String fullName) {
    assert fullName != null;
    ViewInfo viewInfo = viewRepository.getView(fullName);
    return HttpResponse.ofJson(viewInfo);
  }

  @Get("")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #catalog, USE_CATALOG))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse listViews(
      @Param("catalog_name") @AuthorizeKey(CATALOG) String catalogName,
      @Param("schema_name") @AuthorizeKey(SCHEMA) String schemaName) {
    assert catalogName != null;
    assert schemaName != null;
    java.util.List<ViewInfo> views = viewRepository.listViews(catalogName, schemaName);
    return HttpResponse.ofJson(new ListViewsResponse().views(views));
  }

  @Delete("/{full_name}")
  @AuthorizeExpression("""
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #catalog, USE_CATALOG) &&
          #authorize(#principal, #table, OWNER))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse deleteView(@Param("full_name") @AuthorizeKey(TABLE) String fullName) {
    ViewInfo viewInfo = viewRepository.getView(fullName);
    viewRepository.deleteView(fullName);

    SchemaInfo schemaInfo =
        schemaRepository.getSchema(viewInfo.getCatalogName() + "." + viewInfo.getSchemaName());
    removeHierarchicalAuthorizations(viewInfo.getViewId(), schemaInfo.getSchemaId());

    return HttpResponse.of(HttpStatus.OK);
  }
}
