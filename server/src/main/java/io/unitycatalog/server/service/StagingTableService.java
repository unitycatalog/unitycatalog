package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.AuthorizeExpressions;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKeys;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateStagingTable;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.StagingTableRepository;
import io.unitycatalog.server.utils.ServerProperties;

@ExceptionHandler(GlobalExceptionHandler.class)
public class StagingTableService extends AuthorizedService {

  private final StagingTableRepository stagingTableRepository;
  private final SchemaRepository schemaRepository;

  public StagingTableService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties) {
    super(authorizer, repositories, serverProperties);
    this.stagingTableRepository = repositories.getStagingTableRepository();
    this.schemaRepository = repositories.getSchemaRepository();
  }

  static final String disableGoogleJavaFormat =
      """
    Google Java Format and Checkstyle disagree on how to indent annotation array initializers in
    method parameters. This doc string is just a hack to stop Google Java Format from messing up
    with this file.""";

  @Post("")
  @AuthorizeExpression(AuthorizeExpressions.CREATE_STAGING_TABLE)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse createStagingTable(
      @AuthorizeResourceKeys({
        @AuthorizeResourceKey(value = SCHEMA, key = "schema_name"),
        @AuthorizeResourceKey(value = CATALOG, key = "catalog_name")
      })
      CreateStagingTable createStagingTable) {
    assert createStagingTable != null;
    StagingTableInfo createdStagingTable =
        stagingTableRepository.createStagingTable(createStagingTable);
    String catalog = createdStagingTable.getCatalogName();
    String schema = createdStagingTable.getSchemaName();
    initializeHierarchicalAuthorization(
        createdStagingTable.getId(),
        schemaRepository.getSchemaIdOrThrow(catalog, schema).toString());
    return HttpResponse.ofJson(createdStagingTable);
  }
}
