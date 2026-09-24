package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.auth.decorator.AuthorizeValueExtractor;
import io.unitycatalog.server.utils.Constants;
import org.apache.iceberg.rest.requests.UpdateTableRequest;

/**
 * Derives the {@code #table_type} the create policy ({@link
 * io.unitycatalog.server.auth.AuthorizeExpressions#CREATE_TABLE}) keys on for an Iceberg
 * staged-create commit ({@code updateTable}): the location arrives in a {@code set-location}
 * update, and a UC managed location carries the reserved {@code __unitystorage} marker, so a marker
 * path is {@code "MANAGED"} and anything else {@code "EXTERNAL"}. Returns null when there is no
 * set-location (a regular commit, whose policy branch ignores {@code #table_type}).
 *
 * <p>The discriminator is the marker, not the location's presence that {@link
 * IcebergCreateTableTypeExtractor} uses: the location was assigned when the create was staged, so a
 * managed commit still carries one and presence can no longer tell managed from external. The
 * reserved marker still can.
 */
public class IcebergCommitTableTypeExtractor implements AuthorizeValueExtractor {

  @Override
  public Object extract(Object body) {
    String location = IcebergCommitLocationExtractor.lastSetLocation((UpdateTableRequest) body);
    if (location == null || location.isEmpty()) {
      return null;
    }
    return location.contains(Constants.MANAGED_STORAGE_PREFIX) ? "MANAGED" : "EXTERNAL";
  }
}
