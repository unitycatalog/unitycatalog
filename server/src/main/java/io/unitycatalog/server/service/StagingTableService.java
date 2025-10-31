package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeKeys;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateStagingTable;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.StagingTableRepository;

import static io.unitycatalog.server.model.SecurableType.*;

@ExceptionHandler(GlobalExceptionHandler.class)
public class StagingTableService extends AuthorizedService {

  private final StagingTableRepository stagingTableRepository;
  private final SchemaRepository schemaRepository;

  public StagingTableService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    super(authorizer, repositories.getUserRepository());
    this.stagingTableRepository = repositories.getStagingTableRepository();
    this.schemaRepository = repositories.getSchemaRepository();
  }

  @Post("")
  @AuthorizeExpression("""
          (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
           && #authorize(#principal, #schema, OWNER)) ||
          (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
           && #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_TABLE))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse createStagingTable(
      @AuthorizeKeys({
        @AuthorizeKey(value = SCHEMA, key = "schema_name"),
        @AuthorizeKey(value = CATALOG, key = "catalog_name")
      })
      CreateStagingTable createStagingTable) {
    assert createStagingTable != null;
    StagingTableInfo createdStagingTable = stagingTableRepository.createStagingTable(
        createStagingTable);

    SchemaInfo schemaInfo = schemaRepository.getSchema(
        createdStagingTable.getCatalogName() + "." + createdStagingTable.getSchemaName());
    initializeHierarchicalAuthorization(createdStagingTable.getId(), schemaInfo.getSchemaId());
    return HttpResponse.ofJson(createdStagingTable);
  }
}
