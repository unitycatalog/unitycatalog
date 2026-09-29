package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.auth.decorator.AuthorizeValueExtractor;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.rest.requests.UpdateTableRequest;

/**
 * The external storage location a staged-create Iceberg {@code updateTable} commit targets: its
 * single {@code set-location} update, resolving the {@code #external_location} that {@link
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
    // The at-most-one-set-location rule holds for every commit, so validate it here regardless of
    // shape; the returned location is used only for a staged create, since a regular commit is
    // authorized as UPDATE_TABLE and resolves no #external_location.
    String location = requireAtMostOneSetLocation(request);
    return IcebergStagedCreateExtractor.isStagedCreate(request) ? location : null;
  }

  /**
   * The location of the commit's single {@code set-location} update, or null when it has none, so
   * the location this authorizes is unambiguously the one Iceberg applies. Enforced during
   * authorization; the commit handler re-checks it as a backstop for when authorization does not
   * run.
   *
   * @param request the Iceberg commit request
   * @return the single set-location value, or null when the request has none
   * @throws BadRequestException if the request sets the location more than once
   */
  public static String requireAtMostOneSetLocation(UpdateTableRequest request) {
    return request.updates().stream()
        .filter(update -> update instanceof MetadataUpdate.SetLocation)
        .map(update -> ((MetadataUpdate.SetLocation) update).location())
        // reduce's combiner runs only with two or more set-locations, so it is the reject point; a
        // single set-location passes straight through and none yields an empty result.
        .reduce(
            (first, second) -> {
              throw new BadRequestException(
                  "A commit may set the table location at most once, but received more than one"
                      + " set-location update.");
            })
        .orElse(null);
  }
}
