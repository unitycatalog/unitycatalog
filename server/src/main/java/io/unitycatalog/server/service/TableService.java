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
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.persist.*;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TableService extends AuthorizedService {

  private final TableRepository tableRepository;
  private final SchemaRepository schemaRepository;
  private final CatalogRepository catalogRepository;
  private final MetastoreRepository metastoreRepository;

  @SneakyThrows
  public TableService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    super(authorizer, repositories.getUserRepository());
    this.tableRepository = repositories.getTableRepository();
    this.schemaRepository = repositories.getSchemaRepository();
    this.catalogRepository = repositories.getCatalogRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
  }

  @Post("")
  @AuthorizeExpression("""
      (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
          #authorize(#principal, #schema, OWNER)) ||
      (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
          #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_TABLE))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse createTable(
      @AuthorizeKeys({
        @AuthorizeKey(value = SCHEMA, key = "schema_name"),
        @AuthorizeKey(value = CATALOG, key = "catalog_name")
      })
      CreateTable createTable) {
    assert createTable != null;
    TableInfo tableInfo = tableRepository.createTable(createTable);

    SchemaInfo schemaInfo =
        schemaRepository.getSchema(tableInfo.getCatalogName() + "." + tableInfo.getSchemaName());
    initializeHierarchicalAuthorization(tableInfo.getTableId(), schemaInfo.getSchemaId());

    return HttpResponse.ofJson(tableInfo);
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
  public HttpResponse getTable(@Param("full_name") @AuthorizeKey(TABLE) String fullName) {
    assert fullName != null;
    TableInfo tableInfo = tableRepository.getTable(fullName);
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

    ListTablesResponse listTablesResponse = tableRepository.listTables(
        catalogName,
        schemaName,
        maxResults,
        pageToken,
        omitProperties.orElse(false),
        omitColumns.orElse(false));

    filterTables("""
        #authorize(#principal, #metastore, OWNER) ||
        #authorize(#principal, #catalog, OWNER) ||
        (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
        (#authorize(#principal, #schema, USE_SCHEMA) &&
            #authorize(#principal, #catalog, USE_CATALOG) &&
            #authorizeAny(#principal, #table, OWNER, SELECT, MODIFY))
        """, listTablesResponse.getTables());

    return HttpResponse.ofJson(listTablesResponse);
  }

  @Delete("/{full_name}")
  @AuthorizeExpression("""
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #catalog, USE_CATALOG) &&
          #authorize(#principal, #table, OWNER))
      """)
  public HttpResponse deleteTable(@Param("full_name") @AuthorizeKey(TABLE) String fullName) {
    TableInfo tableInfo = tableRepository.getTable(fullName);
    tableRepository.deleteTable(fullName);

    SchemaInfo schemaInfo =
        schemaRepository.getSchema(tableInfo.getCatalogName() + "." + tableInfo.getSchemaName());
    removeHierarchicalAuthorizations(tableInfo.getTableId(), schemaInfo.getSchemaId());

    return HttpResponse.of(HttpStatus.OK);
  }

  public void filterTables(String expression, List<TableInfo> entries) {
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
              TABLE,
              UUID.fromString(ti.getTableId()));
        });
  }
}

