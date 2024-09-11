package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeKeys;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.auth.decorator.UnityAccessUtil;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.Privilege;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.TableRepository;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TableService {

  private static final TableRepository TABLE_REPOSITORY = TableRepository.getInstance();
  private static final SchemaRepository SCHEMA_REPOSITORY = SchemaRepository.getInstance();
  private static final CatalogRepository CATALOG_REPOSITORY = CatalogRepository.getInstance();

  private final UnityCatalogAuthorizer authorizer;
  private final UnityAccessEvaluator evaluator;

  @SneakyThrows
  public TableService(UnityCatalogAuthorizer authorizer) {
    this.authorizer = authorizer;
    evaluator = new UnityAccessEvaluator(authorizer);
  }

  @Post("")
  // The Databricks API does not have a create table endpoint, so defining sensible rules here.
  @AuthorizeExpression("""
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) && #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA)
      """)
  public HttpResponse createTable(
      @AuthorizeKeys({
            @AuthorizeKey(value = SCHEMA, key = "schema_name"),
            @AuthorizeKey(value = CATALOG, key = "catalog_name")
          })
          CreateTable createTable) {
    assert createTable != null;
    TableInfo tableInfo = TABLE_REPOSITORY.createTable(createTable);
    initializeAuthorizations(tableInfo);
    return HttpResponse.ofJson(tableInfo);
  }

  @Get("/{full_name}")
  @AuthorizeExpression("""
          #authorize(#principal, #metastore, METASTORE_ADMIN) ||
          (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
          (#authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG) && #authorizeAny(#principal, #table, OWNER, SELECT))
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse getTable(@Param("full_name") @AuthorizeKey(TABLE) String fullName) {
    assert fullName != null;
    TableInfo tableInfo = TABLE_REPOSITORY.getTable(fullName);
    return HttpResponse.ofJson(tableInfo);
  }

  @Get("")
  @AuthorizeExpression("#defer")
  public HttpResponse listTables(
      @Param("catalog_name") String catalogName,
      @Param("schema_name") String schemaName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken,
      @Param("omit_properties") Optional<Boolean> omitProperties,
      @Param("omit_columns") Optional<Boolean> omitColumns) {

    ListTablesResponse listTablesResponse = TABLE_REPOSITORY.listTables(
            catalogName,
            schemaName,
            maxResults,
            pageToken,
            omitProperties.orElse(false),
            omitColumns.orElse(false));

    filterTables("""
            #authorize(#principal, #metastore, METASTORE_ADMIN) ||
            #authorize(#principal, #table, OWNER) ||
            (#authorize(#principal, #table, SELECT) && #authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG))
            """, listTablesResponse.getTables());

    return HttpResponse.ofJson(listTablesResponse);
  }

  @Delete("/{full_name}")
  @AuthorizeExpression("""
          (#authorizeAll(#principal, #catalog, OWNER, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
          #authorizeAll(#principal, #table, OWNER, USE_CATALOG, USE_SCHEMA)
          """)
  public HttpResponse deleteTable(@Param("full_name") @AuthorizeKey(TABLE) String fullName) {
    TableInfo tableInfo = TABLE_REPOSITORY.getTable(fullName);
    TABLE_REPOSITORY.deleteTable(fullName);
    removeAuthorizations(tableInfo);
    return HttpResponse.of(HttpStatus.OK);
  }

  public void filterTables(String expression, List<TableInfo> entries) {
    // TODO: would be nice to move this to filtering in the Decorator response
    UUID principalId = UnityAccessUtil.findPrincipalId();

    evaluator.filter(
            principalId,
            expression,
            entries,
            ti -> {
              CatalogInfo catalogInfo = CATALOG_REPOSITORY.getCatalog(ti.getCatalogName());
              SchemaInfo schemaInfo =
                      SCHEMA_REPOSITORY.getSchema(ti.getCatalogName() + "." + ti.getSchemaName());
              return Map.of(
                      METASTORE,
                      MetastoreRepository.getInstance().getMetastoreId(),
                      CATALOG,
                      UUID.fromString(catalogInfo.getId()),
                      SCHEMA,
                      UUID.fromString(schemaInfo.getSchemaId()),
                      TABLE,
                      UUID.fromString(ti.getTableId()));
            });
  }

  private void initializeAuthorizations(TableInfo tableInfo) {
    SchemaInfo schemaInfo =
        SCHEMA_REPOSITORY.getSchema(tableInfo.getCatalogName() + "." + tableInfo.getSchemaName());
    UUID principalId = UnityAccessUtil.findPrincipalId();
    // add owner privilege
    authorizer.grantAuthorization(
        principalId, UUID.fromString(tableInfo.getTableId()), Privilege.OWNER);
    // make table a child of the schema
    authorizer.addHierarchyChild(
        UUID.fromString(schemaInfo.getSchemaId()), UUID.fromString(tableInfo.getTableId()));
  }

  private void removeAuthorizations(TableInfo tableInfo) {
    SchemaInfo schemaInfo =
        SCHEMA_REPOSITORY.getSchema(tableInfo.getCatalogName() + "." + tableInfo.getSchemaName());
    // remove any direct authorizations on the table
    authorizer.clearAuthorizationsForResource(UUID.fromString(tableInfo.getTableId()));
    // remove link to the parent schema
    authorizer.removeHierarchyChild(
        UUID.fromString(schemaInfo.getSchemaId()), UUID.fromString(tableInfo.getTableId()));
  }
}
