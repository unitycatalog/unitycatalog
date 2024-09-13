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
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.FunctionRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.IdentityUtils;
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

  private static final FunctionRepository FUNCTION_REPOSITORY = FunctionRepository.getInstance();

  private static final SchemaRepository SCHEMA_REPOSITORY = SchemaRepository.getInstance();
  private static final CatalogRepository CATALOG_REPOSITORY = CatalogRepository.getInstance();

  private final UnityCatalogAuthorizer authorizer;
  private final UnityAccessEvaluator evaluator;

  @SneakyThrows
  public FunctionService(UnityCatalogAuthorizer authorizer) {
    this.authorizer = authorizer;
    evaluator = new UnityAccessEvaluator(authorizer);
  }

  @Post("")
  // TODO: for now, we are not supporting CREATE VOLUME or CREATE EXTERNAL VOLUME privileges
  @AuthorizeExpression("""
          #authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, USE_SCHEMA)
          """)
  public HttpResponse createFunction(@AuthorizeKeys({
                                        @AuthorizeKey(value = CATALOG, key = "function_info.catalog_name"),
                                        @AuthorizeKey(value = SCHEMA, key = "function_info.schema_name")
                                      })
                                      CreateFunctionRequest createFunctionRequest) {
    FunctionInfo functionInfo = FUNCTION_REPOSITORY.createFunction(createFunctionRequest);
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

    ListFunctionsResponse listFunctionsResponse = FUNCTION_REPOSITORY.listFunctions(catalogName, schemaName, maxResults, pageToken);
    filterVolumes("""
            #authorize(#principal, #metastore, OWNER) ||
            #authorize(#principal, #function, OWNER) ||
            (#authorizeAny(#principal, #function, EXECUTE) && #authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG))
            """, listFunctionsResponse.getFunctions());
    return HttpResponse.ofJson(listFunctionsResponse);
  }

  @Get("/{name}")
  @AuthorizeKey(METASTORE)
  @AuthorizeExpression("""
          #authorize(#principal, #metastore, OWNER) ||
          #authorize(#principal, #catalog, OWNER) ||
          (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #function, OWNER)) ||
          (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #function, EXECUTE))
          """)
  public HttpResponse getFunction(@Param("name") @AuthorizeKey(FUNCTION) String name) {
    return HttpResponse.ofJson(FUNCTION_REPOSITORY.getFunction(name));
  }

  @Delete("/{name}")
  @AuthorizeKey(METASTORE)
  @AuthorizeExpression("""
          #authorize(#principal, #metastore, OWNER) ||
          (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
          (#authorize(#principal, #function, OWNER) && #authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG))
          """)
  public HttpResponse deleteFunction(
      @Param("name") @AuthorizeKey(FUNCTION) String name, @Param("force") Optional<Boolean> force) {
    FunctionInfo functionInfo = FUNCTION_REPOSITORY.getFunction(name);
    FUNCTION_REPOSITORY.deleteFunction(name, force.orElse(false));
    removeAuthorizations(functionInfo);
    return HttpResponse.of(HttpStatus.OK);
  }

  public void filterVolumes(String expression, List<FunctionInfo> entries) {
    // TODO: would be nice to move this to filtering in the Decorator response
    UUID principalId = IdentityUtils.findPrincipalId();

    evaluator.filter(
            principalId,
            expression,
            entries,
            fi -> {
              CatalogInfo catalogInfo = CATALOG_REPOSITORY.getCatalog(fi.getCatalogName());
              SchemaInfo schemaInfo =
                      SCHEMA_REPOSITORY.getSchema(fi.getCatalogName() + "." + fi.getSchemaName());
              return Map.of(
                      METASTORE,
                      MetastoreRepository.getInstance().getMetastoreId(),
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
            SCHEMA_REPOSITORY.getSchema(functionInfo.getCatalogName() + "." + functionInfo.getSchemaName());
    UUID principalId = IdentityUtils.findPrincipalId();
    // add owner privilege
    authorizer.grantAuthorization(
            principalId, UUID.fromString(functionInfo.getFunctionId()), Privileges.OWNER);
    // make table a child of the schema
    authorizer.addHierarchyChild(
            UUID.fromString(schemaInfo.getSchemaId()), UUID.fromString(functionInfo.getFunctionId()));
  }

  private void removeAuthorizations(FunctionInfo functionInfo) {
    SchemaInfo schemaInfo =
            SCHEMA_REPOSITORY.getSchema(functionInfo.getCatalogName() + "." + functionInfo.getSchemaName());
    // remove any direct authorizations on the table
    authorizer.clearAuthorizationsForResource(UUID.fromString(functionInfo.getFunctionId()));
    // remove link to the parent schema
    authorizer.removeHierarchyChild(
            UUID.fromString(schemaInfo.getSchemaId()), UUID.fromString(functionInfo.getFunctionId()));
  }

}
