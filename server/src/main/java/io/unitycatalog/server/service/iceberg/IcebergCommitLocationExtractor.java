package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.auth.decorator.AuthorizeValueExtractor;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.rest.requests.UpdateTableRequest;

/**
 * The external storage location an Iceberg {@code updateTable} commit targets: the last {@code
 * set-location} update, or null when the commit has none. Resolves the {@code #external_location}
 * that {@link io.unitycatalog.server.auth.AuthorizeExpressions#CREATE_TABLE} authorizes for an
 * external staged create; a managed commit keys on {@code #table_type} instead and never consults
 * it.
 */
public class IcebergCommitLocationExtractor implements AuthorizeValueExtractor {

  @Override
  public Object extract(Object body) {
    return lastSetLocation((UpdateTableRequest) body);
  }

  /** The location of the request's last {@code set-location} update, or null when there is none. */
  static String lastSetLocation(UpdateTableRequest request) {
    return request.updates().stream()
        .filter(update -> update instanceof MetadataUpdate.SetLocation)
        .map(update -> ((MetadataUpdate.SetLocation) update).location())
        // Iceberg applies updates in order, so the last set-location wins.
        .reduce((first, second) -> second)
        .orElse(null);
  }
}
