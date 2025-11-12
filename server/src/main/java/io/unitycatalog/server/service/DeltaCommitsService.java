package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeKeys;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.Commit;
import io.unitycatalog.server.persist.DeltaCommitRepository;
import io.unitycatalog.server.persist.Repositories;
import lombok.SneakyThrows;

import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.TABLE;

/**
 * REST API service for coordinated commits to Delta tables in Unity Catalog.
 */
@ExceptionHandler(GlobalExceptionHandler.class)
public class DeltaCommitsService extends AuthorizedService {

  private final DeltaCommitRepository deltaCommitRepository;

  @SneakyThrows
  public DeltaCommitsService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    super(authorizer, repositories.getUserRepository());
    this.deltaCommitRepository = repositories.getDeltaCommitRepository();
  }

  @Post("")
  @AuthorizeExpression("""
        #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
        #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
        #authorizeAny(#principal, #table, OWNER, MODIFY)
      """)
  @AuthorizeKey(METASTORE)
  public HttpResponse postCommit(
      @AuthorizeKeys({@AuthorizeKey(value = TABLE, key = "table_id")})
      Commit commit) {
    deltaCommitRepository.postCommit(commit);
    return HttpResponse.of(HttpStatus.OK);
  }

  // TODO: implement GET
}
