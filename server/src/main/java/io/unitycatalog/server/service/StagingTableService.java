package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeKeys;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateStagingTable;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.persist.StagingTableRepository;

import static io.unitycatalog.server.model.SecurableType.*;

@ExceptionHandler(GlobalExceptionHandler.class)
public class StagingTableService {
  private static final StagingTableRepository STAGING_TABLE_REPOSITORY = StagingTableRepository.getInstance();

  public StagingTableService() {}

  @Post("")
  @AuthorizeExpression("""
          (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
          (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) && #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_TABLE))
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse createStagingTable(@AuthorizeKeys({
          @AuthorizeKey(value = SCHEMA, key = "schema_name"),
          @AuthorizeKey(value = CATALOG, key = "catalog_name")
  }) CreateStagingTable createStagingTable) {
    assert createStagingTable != null;
    StagingTableInfo createdStagingTable = STAGING_TABLE_REPOSITORY.createStagingTable(createStagingTable);
    return HttpResponse.ofJson(createdStagingTable);
  }
}
