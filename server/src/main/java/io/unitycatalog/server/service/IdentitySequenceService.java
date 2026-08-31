package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.TABLE;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.AuthorizeExpressions;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.model.CreateIdentitySequences;
import io.unitycatalog.server.model.DropIdentitySequences;
import io.unitycatalog.server.model.ReserveIdentityRanges;
import io.unitycatalog.server.persist.IdentitySequenceRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.utils.ServerProperties;
import lombok.SneakyThrows;

/**
 * REST API service for concurrent identity column sequences.
 *
 * <p>All three operations, create, reserve, and drop, mutate identity state on a table, so each
 * requires {@code MODIFY} on the table (via {@link AuthorizeExpressions#UPDATE_TABLE}).
 */
public class IdentitySequenceService extends AuthorizedService implements UnityCatalogRestService {

  private final IdentitySequenceRepository identitySequenceRepository;

  @SneakyThrows
  public IdentitySequenceService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties) {
    super(authorizer, repositories, serverProperties);
    this.identitySequenceRepository = repositories.getIdentitySequenceRepository();
  }

  @Post("")
  @AuthorizeExpression(AuthorizeExpressions.UPDATE_TABLE)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse createSequences(
      @AuthorizeResourceKey(value = TABLE, key = "table_id") CreateIdentitySequences request) {
    serverProperties.checkIdentitySequencesEnabled();
    return HttpResponse.ofJson(identitySequenceRepository.createSequences(request));
  }

  @Post("/reserve")
  @AuthorizeExpression(AuthorizeExpressions.UPDATE_TABLE)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse reserveRanges(
      @AuthorizeResourceKey(value = TABLE, key = "table_id") ReserveIdentityRanges request) {
    serverProperties.checkIdentitySequencesEnabled();
    return HttpResponse.ofJson(identitySequenceRepository.reserveRanges(request));
  }

  @Post("/drop")
  @AuthorizeExpression(AuthorizeExpressions.UPDATE_TABLE)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse dropSequences(
      @AuthorizeResourceKey(value = TABLE, key = "table_id") DropIdentitySequences request) {
    serverProperties.checkIdentitySequencesEnabled();
    return HttpResponse.ofJson(identitySequenceRepository.dropSequences(request));
  }
}
