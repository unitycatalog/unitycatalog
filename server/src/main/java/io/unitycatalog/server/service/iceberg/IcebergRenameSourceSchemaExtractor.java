package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.auth.decorator.AuthorizeValueExtractor;
import org.apache.iceberg.rest.requests.RenameTableRequest;

/**
 * The schema (namespace) of an Iceberg {@code renameTable} request's source table, resolving the
 * {@code #schema} that {@link io.unitycatalog.server.auth.AuthorizeExpressions#RENAME_TABLE}
 * authorizes. Returns null when the request has no source (rejected later by {@code validate()}),
 * leaving the key unresolved rather than raising during authorization.
 */
public class IcebergRenameSourceSchemaExtractor implements AuthorizeValueExtractor {

  @Override
  public Object extract(Object body) {
    RenameTableRequest request = (RenameTableRequest) body;
    return request.source() == null ? null : request.source().namespace().toString();
  }
}
