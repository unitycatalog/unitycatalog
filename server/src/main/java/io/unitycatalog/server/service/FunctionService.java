package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.FUNCTION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.ResponseAuthorizeFilter;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKeys;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateFunctionRequest;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.FunctionInfo;
import io.unitycatalog.server.model.ListFunctionsResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.FunctionRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Optional;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import lombok.SneakyThrows;

@ExceptionHandler(GlobalExceptionHandler.class)
public class FunctionService extends AuthorizedService {

  private final FunctionRepository functionRepository;
  private final SchemaRepository schemaRepository;
  private final CatalogRepository catalogRepository;
  private final MetastoreRepository metastoreRepository;

  @SneakyThrows
  public FunctionService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties) {
    super(authorizer, repositories, serverProperties);
    this.catalogRepository = repositories.getCatalogRepository();
    this.schemaRepository = repositories.getSchemaRepository();
    this.functionRepository = repositories.getFunctionRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
  }

  @Post("")
  // TODO: for now, we are not supporting CREATE FUNCTION privilege
  @AuthorizeExpression("""
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
          #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA)
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse createFunction(
      @AuthorizeResourceKeys({
        @AuthorizeResourceKey(value = CATALOG, key = "function_info.catalog_name"),
        @AuthorizeResourceKey(value = SCHEMA, key = "function_info.schema_name")
      })
      CreateFunctionRequest createFunctionRequest) {
    FunctionInfo functionInfo = functionRepository.createFunction(createFunctionRequest);

    String catalogName = functionInfo.getCatalogName();
    String schemaName = functionInfo.getSchemaName();
    SchemaInfo schemaInfo = schemaRepository.getSchema(catalogName + "." + schemaName);
    initializeHierarchicalAuthorization(functionInfo.getFunctionId(), schemaInfo.getSchemaId());

    return HttpResponse.ofJson(functionInfo);
  }

  @Get("")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) &&
          #authorize(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorizeAny(#principal, #catalog, USE_CATALOG) &&
          #authorizeAny(#principal, #function, OWNER, EXECUTE))
      """)
  @ResponseAuthorizeFilter
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse listFunctions(
      @Param("catalog_name") @AuthorizeResourceKey(CATALOG) String catalogName,
      @Param("schema_name") @AuthorizeResourceKey(SCHEMA) String schemaName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {
    ListFunctionsResponse listFunctionsResponse =
        functionRepository.listFunctions(catalogName, schemaName, maxResults, pageToken);
    applyResponseFilter(SecurableType.FUNCTION, listFunctionsResponse.getFunctions());
    return HttpResponse.ofJson(listFunctionsResponse);
  }

  @Get("/{name}")
  @AuthorizeResourceKey(METASTORE)
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) &&
          #authorizeAny(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #catalog, USE_CATALOG) &&
          #authorize(#principal, #schema, USE_SCHEMA) &&
          #authorizeAny(#principal, #function, OWNER, EXECUTE))
      """)
  public HttpResponse getFunction(@Param("name") @AuthorizeResourceKey(FUNCTION) String name) {
    return HttpResponse.ofJson(functionRepository.getFunction(name));
  }

  @Delete("/{name}")
  @AuthorizeResourceKey(METASTORE)
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      (#authorize(#principal, #function, OWNER) &&
          #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
          #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG))
      """)
  public HttpResponse deleteFunction(
      @Param("name") @AuthorizeResourceKey(FUNCTION) String name,
      @Param("force") Optional<Boolean> force) {
    FunctionInfo functionInfo = functionRepository.getFunction(name);
    functionRepository.deleteFunction(name, force.orElse(false));

    String catalogName = functionInfo.getCatalogName();
    String schemaName = functionInfo.getSchemaName();
    SchemaInfo schemaInfo = schemaRepository.getSchema(catalogName + "." + schemaName);
    removeHierarchicalAuthorizations(functionInfo.getFunctionId(), schemaInfo.getSchemaId());

    return HttpResponse.of(HttpStatus.OK);
  }

}

