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
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CreateFunctionRequest;
import io.unitycatalog.server.model.FunctionInfo;
import io.unitycatalog.server.model.ListFunctionsResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.persist.*;
import io.unitycatalog.server.persist.model.Privileges;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.FUNCTION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;

@ExceptionHandler(GlobalExceptionHandler.class)
public class FunctionService {

  private final FunctionRepository functionRepository;
  private final SchemaRepository schemaRepository;
  private final CatalogRepository catalogRepository;
  private final MetastoreRepository metastoreRepository;
  private final UserRepository userRepository;

  private final UnityCatalogAuthorizer authorizer;
  private final UnityAccessEvaluator evaluator;

  @SneakyThrows
  public FunctionService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    this.authorizer = authorizer;
    this.evaluator = new UnityAccessEvaluator(authorizer);
    this.catalogRepository = repositories.getCatalogRepository();
    this.schemaRepository = repositories.getSchemaRepository();
    this.functionRepository = repositories.getFunctionRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
    this.userRepository = repositories.getUserRepository();
  }

  @Post("")
  // TODO: for now, we are not supporting CREATE FUNCTION privilege
  @AuthorizeExpression("""
          #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) && #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA)
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse createFunction(@AuthorizeKeys({
                                        @AuthorizeKey(value = CATALOG, key = "function_info.catalog_name"),
                                        @AuthorizeKey(value = SCHEMA, key = "function_info.schema_name")
                                      })
                                      CreateFunctionRequest createFunctionRequest) {
    FunctionInfo functionInfo = functionRepository.createFunction(createFunctionRequest);
    initializeAuthorizations(functionInfo);
    return HttpResponse.ofJson(functionInfo);
  }

  @Get("")
  @AuthorizeExpression("#defer")
  public HttpResponse listFunctions(
      @Param("catalog_name") String catalogName,
      @Param("schema_name") String schemaName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {

    ListFunctionsResponse listFunctionsResponse = functionRepository.listFunctions(catalogName, schemaName, maxResults, pageToken);
    filterFunctions("""
            #authorize(#principal, #metastore, OWNER) ||
            #authorize(#principal, #catalog, OWNER) ||
            (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
            (#authorize(#principal, #schema, USE_SCHEMA) && #authorizeAny(#principal, #catalog, USE_CATALOG) && #authorizeAny(#principal, #function, OWNER, EXECUTE))
            """, listFunctionsResponse.getFunctions());
    return HttpResponse.ofJson(listFunctionsResponse);
  }

  @Get("/{name}")
  @AuthorizeKey(METASTORE)
  @AuthorizeExpression("""
          #authorize(#principal, #metastore, OWNER) ||
          #authorize(#principal, #catalog, OWNER) ||
          (#authorize(#principal, #schema, OWNER) && #authorizeAny(#principal, #catalog, USE_CATALOG)) ||
          (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, USE_SCHEMA) && #authorizeAny(#principal, #function, OWNER, EXECUTE))
          """)
  public HttpResponse getFunction(@Param("name") @AuthorizeKey(FUNCTION) String name) {
    return HttpResponse.ofJson(functionRepository.getFunction(name));
  }

  @Delete("/{name}")
  @AuthorizeKey(METASTORE)
  @AuthorizeExpression("""
          #authorize(#principal, #metastore, OWNER) ||
          (#authorize(#principal, #function, OWNER) && #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) && #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG))
          """)
  public HttpResponse deleteFunction(
      @Param("name") @AuthorizeKey(FUNCTION) String name, @Param("force") Optional<Boolean> force) {
    FunctionInfo functionInfo = functionRepository.getFunction(name);
    functionRepository.deleteFunction(name, force.orElse(false));
    removeAuthorizations(functionInfo);
    return HttpResponse.of(HttpStatus.OK);
  }

  public void filterFunctions(String expression, List<FunctionInfo> entries) {
    // TODO: would be nice to move this to filtering in the Decorator response
    UUID principalId = userRepository.findPrincipalId();

    evaluator.filter(
            principalId,
            expression,
            entries,
            fi -> {
              CatalogInfo catalogInfo = catalogRepository.getCatalog(fi.getCatalogName());
              SchemaInfo schemaInfo =
                      schemaRepository.getSchema(fi.getCatalogName() + "." + fi.getSchemaName());
              return Map.of(
                      METASTORE,
                      metastoreRepository.getMetastoreId(),
                      CATALOG,
                      UUID.fromString(catalogInfo.getId()),
                      SCHEMA,
                      UUID.fromString(schemaInfo.getSchemaId()),
                      FUNCTION,
                      UUID.fromString(fi.getFunctionId()));
            });
  }

  private void initializeAuthorizations(FunctionInfo functionInfo) {
    SchemaInfo schemaInfo =
            schemaRepository.getSchema(functionInfo.getCatalogName() + "." + functionInfo.getSchemaName());
    UUID principalId = userRepository.findPrincipalId();
    // add owner privilege
    authorizer.grantAuthorization(
            principalId, UUID.fromString(functionInfo.getFunctionId()), Privileges.OWNER);
    // make table a child of the schema
    authorizer.addHierarchyChild(
            UUID.fromString(schemaInfo.getSchemaId()), UUID.fromString(functionInfo.getFunctionId()));
  }

  private void removeAuthorizations(FunctionInfo functionInfo) {
    SchemaInfo schemaInfo =
            schemaRepository.getSchema(functionInfo.getCatalogName() + "." + functionInfo.getSchemaName());
    // remove any direct authorizations on the table
    authorizer.clearAuthorizationsForResource(UUID.fromString(functionInfo.getFunctionId()));
    // remove link to the parent schema
    authorizer.removeHierarchyChild(
            UUID.fromString(schemaInfo.getSchemaId()), UUID.fromString(functionInfo.getFunctionId()));
  }

}
