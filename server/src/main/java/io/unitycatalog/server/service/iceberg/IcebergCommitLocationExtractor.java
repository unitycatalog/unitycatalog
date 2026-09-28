package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.auth.decorator.AuthorizeValueExtractor;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.rest.requests.UpdateTableRequest;

/**
 * The external storage location a staged-create Iceberg {@code updateTable} commit targets: the
 * last {@code set-location} update, resolving the {@code #external_location} that {@link
 * io.unitycatalog.server.auth.AuthorizeExpressions#CREATE_TABLE} authorizes.
 *
 * <p>Returns null for a regular (non staged-create) commit: it is authorized as {@code
 * UPDATE_TABLE} (table tier), which does not consult {@code #external_location}, and its location
 * cannot change. Resolving a caller-supplied {@code set-location} there would only let {@code
 * KeyMapper} overwrite the URL-keyed table id with the id of whatever entity owns that path.
 */
public class IcebergCommitLocationExtractor implements AuthorizeValueExtractor {

  @Override
  public Object extract(Object body) {
    UpdateTableRequest request = (UpdateTableRequest) body;
    if (!IcebergStagedCreateExtractor.isStagedCreate(request)) {
      return null;
    }
    return lastSetLocation(request);
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
