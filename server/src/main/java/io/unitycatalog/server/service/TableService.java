package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.EXTERNAL_LOCATION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Patch;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.AuthorizeExpressions;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKeys;
import io.unitycatalog.server.auth.annotation.ResponseAuthorizeFilter;
import io.unitycatalog.server.auth.decorator.BrowseOnlyConverter;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableExistsResponse;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.UpdateTable;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;

public class TableService extends AuthorizedService implements UnityCatalogRestService {

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
  @AuthorizeExpression(AuthorizeExpressions.CREATE_TABLE)
  public HttpResponse createTable(
      @AuthorizeResourceKeys({
            @AuthorizeResourceKey(value = SCHEMA, key = "schema_name"),
            @AuthorizeResourceKey(value = CATALOG, key = "catalog_name"),
            @AuthorizeResourceKey(value = EXTERNAL_LOCATION, key = "storage_location")
          })
          @AuthorizeKey(key = "table_type")
          CreateTable createTable) {
    assert createTable != null;
    serverProperties.checkDeltaApiOnlyForManagedTable(
        createTable.getTableType(), "POST /delta/v1/catalogs/{catalog}/schemas/{schema}/tables");
    TableInfo tableInfo = tableRepository.createTable(createTable);

    SchemaInfo schemaInfo =
        schemaRepository.getSchema(tableInfo.getCatalogName() + "." + tableInfo.getSchemaName());
    initializeHierarchicalAuthorization(tableInfo.getTableId(), schemaInfo.getSchemaId());

    return HttpResponse.ofJson(tableInfo);
  }

  @Get("/{full_name}")
  @AuthorizeExpression(AuthorizeExpressions.GET_TABLE)
  @ResponseAuthorizeFilter(isList = false, browse = AuthorizeExpressions.BROWSE_CATALOG)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse getTable(
      @Param("full_name") @AuthorizeResourceKey(TABLE) String fullName,
      @Param("include_browse") Optional<Boolean> includeBrowse) {
    assert fullName != null;
    TableInfo tableInfo = tableRepository.getTable(fullName);
    return HttpResponse.ofJson(
        applyGetResponseFilter(SecurableType.TABLE, tableInfo, BrowseOnlyConverter::toBrowseOnly));
  }

  /**
   * Checks a regular table's existence using the same visibility policy as table metadata reads.
   *
   * @param fullName the three-part table name
   * @return a boolean response, with permission failures preserved for existing tables
   */
  @Get("/{full_name}/exists")
  @AuthorizeExpression(
      "("
          + AuthorizeExpressions.GET_TABLE
          + ") || "
          + AuthorizeExpressions.BROWSE_CATALOG
          + " || #authorize(#principal, #schema, BROWSE)")
  @ResponseAuthorizeFilter
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse tableExists(@Param("full_name") String fullName) {
    String[] parts = ValidationUtils.splitFullName(fullName, 3, "Invalid table name: " + fullName);
    for (String part : parts) {
      ValidationUtils.validateSqlObjectName(part);
    }
    // Resolve the table in the handler so an absent ancestor can also return false.
    List<TableInfoDAO> tables = new ArrayList<>();
    try {
      tables.add(tableRepository.findTableOrThrow(parts[0], parts[1], parts[2]));
    } catch (BaseException e) {
      switch (e.getErrorCode()) {
        case CATALOG_NOT_FOUND, SCHEMA_NOT_FOUND, TABLE_NOT_FOUND -> {}
        default -> throw e;
      }
    }
    boolean exists = !tables.isEmpty();
    applyResponseFilter(TABLE, tables);
    if (exists && tables.isEmpty()) {
      throw new BaseException(ErrorCode.PERMISSION_DENIED, "Access denied.");
    }
    return HttpResponse.ofJson(new TableExistsResponse().tableExists(exists));
  }

  @Get("")
  @AuthorizeExpression(AuthorizeExpressions.GET_TABLE)
  @ResponseAuthorizeFilter(browse = AuthorizeExpressions.BROWSE_CATALOG)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse listTables(
      @Param("catalog_name") @AuthorizeResourceKey(CATALOG) String catalogName,
      @Param("schema_name") @AuthorizeResourceKey(SCHEMA) String schemaName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken,
      @Param("omit_properties") Optional<Boolean> omitProperties,
      @Param("omit_columns") Optional<Boolean> omitColumns,
      @Param("include_browse") Optional<Boolean> includeBrowse) {

    ListTablesResponse listTablesResponse =
        tableRepository.listTables(
            catalogName,
            schemaName,
            maxResults,
            pageToken,
            omitProperties.orElse(false),
            omitColumns.orElse(false));

    applyResponseFilter(
        SecurableType.TABLE, listTablesResponse.getTables(), BrowseOnlyConverter::toBrowseOnly);
    return HttpResponse.ofJson(listTablesResponse);
  }

  @Patch("/{full_name}")
  @AuthorizeExpression(
      "#authorize(#principal, #metastore, OWNER) || (" + AuthorizeExpressions.DELETE_TABLE + ")")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateTable(
      @Param("full_name") @AuthorizeResourceKey(TABLE) String fullName, UpdateTable updateTable) {
    boolean canTransferViewToRecipient = canTransferOwnerTo(updateTable.getOwner());
    TableInfo result =
        updateWithOwnerReload(
            updateTable.getOwner(),
            beforeCommit ->
                tableRepository.updateTable(
                    fullName, updateTable, canTransferViewToRecipient, beforeCommit));
    return HttpResponse.ofJson(result);
  }

  @Delete("/{full_name}")
  @AuthorizeExpression(AuthorizeExpressions.DELETE_TABLE)
  public HttpResponse deleteTable(
      @Param("full_name") @AuthorizeResourceKey(TABLE) String fullName) {
    TableInfoDAO deleted = tableRepository.deleteTable(fullName);
    removeHierarchicalAuthorizations(deleted.getId().toString(), deleted.getSchemaId().toString());
    return HttpResponse.of(HttpStatus.OK);
  }
}
