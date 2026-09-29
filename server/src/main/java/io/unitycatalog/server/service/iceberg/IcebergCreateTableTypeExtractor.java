package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.auth.decorator.AuthorizeValueExtractor;
import org.apache.iceberg.rest.requests.CreateTableRequest;

/**
 * Derives the {@code #table_type} the create policy ({@link
 * io.unitycatalog.server.auth.AuthorizeExpressions#CREATE_TABLE}) keys on for Iceberg {@code
 * createTable}, which carries no explicit table type: a managed create omits the location (the
 * server assigns it), so a missing location is {@code "MANAGED"} and a provided location is {@code
 * "EXTERNAL"}.
 *
 * <p>The discriminator is the location's presence, not the {@code __unitystorage} marker that
 * {@link IcebergCommitTableTypeExtractor} matches: a managed create supplies no location at all
 * (the server assigns it afterward), so absence alone means managed and there is no marker to look
 * for yet. Once that create is committed through {@code updateTable} the location has been assigned
 * and is always present, so the commit path switches to marker matching.
 */
public class IcebergCreateTableTypeExtractor implements AuthorizeValueExtractor {

  @Override
  public Object extract(Object body) {
    String location = ((CreateTableRequest) body).location();
    return (location == null || location.isEmpty()) ? "MANAGED" : "EXTERNAL";
  }
}
