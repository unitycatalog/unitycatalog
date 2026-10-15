package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.auth.decorator.AuthorizeValueExtractor;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.rest.requests.RenameTableRequest;

/**
 * The schema (namespace) of an Iceberg {@code renameTable} request's source table, resolving the
 * {@code #schema} that {@link io.unitycatalog.server.auth.AuthorizeExpressions#RENAME_TABLE}
 * authorizes. Fails fast with a 400 when the request carries no source table, so a malformed
 * request is rejected during authorization instead of resolving a {@code <catalog>.null} schema and
 * surfacing as a 404.
 */
public class IcebergRenameSourceSchemaExtractor implements AuthorizeValueExtractor {

  @Override
  public Object extract(Object body) {
    RenameTableRequest request = (RenameTableRequest) body;
    if (request.source() == null) {
      throw new BadRequestException("Rename request must specify a source table.");
    }
    return request.source().namespace().toString();
  }
}
