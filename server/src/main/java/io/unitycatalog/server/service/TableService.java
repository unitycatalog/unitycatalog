package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.EXTERNAL_LOCATION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.ResponseAuthorizeFilter;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKeys;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.TableRepository;
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
public class TableService extends AuthorizedService {

  private final TableRepository tableRepository;
  private final SchemaRepository schemaRepository;

  @SneakyThrows
  public TableService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties) {
    super(authorizer, repositories, serverProperties);
    this.tableRepository = repositories.getTableRepository();
    this.schemaRepository = repositories.getSchemaRepository();
  }

  /**
   * Creates a new table in the specified schema.
   *
   * <p>Authorization requirements:
   *
   * <ul>
   *   <li>Must have OWNER or USE_CATALOG permission on the catalog
   *   <li>Must have either OWNER permission on the schema, or both USE_SCHEMA and CREATE_TABLE
   *       permissions
   *   <li>For EXTERNAL tables:
   *       <ul>
   *         <li>The storage location must not overlap with any existing table, volume, or
   *             registered model
   *         <li>If the storage location falls within a registered external location, the user must
   *             have OWNER or CREATE_EXTERNAL_TABLE permission on that external location
   *         <li>If the storage location does not fall within any registered external location, the
   *             table can be created without additional external location permissions
   *       </ul>
   *   <li>MANAGED table creation delegates to catalog and schema for permission. Once the catalog
   *       or schema is allowed to create under an external location with permission
   *       CREATE_MANAGED_STORAGE, all managed entity creations under it no longer need to check for
   *       external location again.
   * </ul>
   *
   * @param createTable the table creation request containing table metadata and storage info
   * @return HTTP response containing the created TableInfo
   */
  @Post("")
  @AuthorizeExpression("""
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      (#authorize(#principal, #schema, OWNER) ||
        #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_TABLE)) &&
      (#table_type != 'EXTERNAL' ||
        (#no_overlap_with_data_securable &&
          (#external_location == null ||
           #authorizeAny(#principal, #external_location, OWNER, CREATE_EXTERNAL_TABLE))))
      """)
  public HttpResponse createTable(
      @AuthorizeResourceKeys({
        @AuthorizeResourceKey(value = SCHEMA, key = "schema_name"),
        @AuthorizeResourceKey(value = CATALOG, key = "catalog_name"),
        @AuthorizeResourceKey(value = EXTERNAL_LOCATION, key = "storage_location")
      })
      @AuthorizeKey(key = "table_type")
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
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse getTable(@Param("full_name") @AuthorizeResourceKey(TABLE) String fullName) {
    assert fullName != null;
    TableInfo tableInfo = tableRepository.getTable(fullName);
    return HttpResponse.ofJson(tableInfo);
  }

  @Get("")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) &&
          #authorize(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #catalog, USE_CATALOG) &&
          #authorizeAny(#principal, #table, OWNER, SELECT, MODIFY))
      """)
  @ResponseAuthorizeFilter
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse listTables(
      @Param("catalog_name") @AuthorizeResourceKey(CATALOG) String catalogName,
      @Param("schema_name") @AuthorizeResourceKey(SCHEMA) String schemaName,
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

    applyResponseFilter(SecurableType.TABLE, listTablesResponse.getTables());
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
  public HttpResponse deleteTable(
      @Param("full_name") @AuthorizeResourceKey(TABLE) String fullName) {
    TableInfo tableInfo = tableRepository.getTable(fullName);
    tableRepository.deleteTable(fullName);

    SchemaInfo schemaInfo =
        schemaRepository.getSchema(tableInfo.getCatalogName() + "." + tableInfo.getSchemaName());
    removeHierarchicalAuthorizations(tableInfo.getTableId(), schemaInfo.getSchemaId());

    return HttpResponse.of(HttpStatus.OK);
  }
}
