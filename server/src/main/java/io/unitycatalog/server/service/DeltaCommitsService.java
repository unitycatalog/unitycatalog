package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.AuthorizeExpressions;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.DeltaCommit;
import io.unitycatalog.server.model.DeltaGetCommits;
import io.unitycatalog.server.persist.DeltaCommitRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.utils.ServerProperties;
import lombok.SneakyThrows;

import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.TABLE;

/**
 * REST API service for Delta commits to Delta tables in Unity Catalog.
 */
@ExceptionHandler(GlobalExceptionHandler.class)
public class DeltaCommitsService extends AuthorizedService {

  private final DeltaCommitRepository deltaCommitRepository;

  @SneakyThrows
  public DeltaCommitsService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties) {
    super(authorizer, repositories, serverProperties);
    this.deltaCommitRepository = repositories.getDeltaCommitRepository();
  }

  @Post("")
  @AuthorizeExpression(AuthorizeExpressions.UPDATE_TABLE)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse postCommit(
      @AuthorizeResourceKey(value = TABLE, key = "table_id")
      DeltaCommit commit) {
    // The UC REST commit endpoint only accepts MANAGED Delta tables (enforced downstream by
    // validateTableForCommit), so the gate applies unconditionally here.
    serverProperties.checkDeltaApiOnlyEnabled(
        "POST /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table} with action add-commit");
    deltaCommitRepository.postCommit(commit);
    return HttpResponse.of(HttpStatus.OK);
  }

  @Get("")
  @AuthorizeExpression("""
        #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
        #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
        #authorizeAny(#principal, #table, OWNER, SELECT)
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse getCommits(
    @AuthorizeResourceKey(value = TABLE, key = "table_id")
    DeltaGetCommits rpc) {
    // Same as postCommit above -- managed-Delta-only by contract; gate unconditionally.
    serverProperties.checkDeltaApiOnlyEnabled(
        "GET /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table} (commits[])");
    return HttpResponse.ofJson(deltaCommitRepository.getCommits(rpc));
  }
}
