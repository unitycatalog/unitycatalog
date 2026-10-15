package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.auth.decorator.AuthorizeValueExtractor;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.rest.requests.UpdateTableRequest;

/**
 * Supplies the {@code #staged_create} flag for the Iceberg {@code updateTable} policy ({@link
 * io.unitycatalog.server.auth.AuthorizeExpressions#UPDATE_ICEBERG_TABLE}): true authorizes the
 * commit as a table creation, false as an update. Delegates to {@link #isStagedCreate}.
 */
public class IcebergStagedCreateExtractor implements AuthorizeValueExtractor {

  @Override
  public Object extract(Object body) {
    return isStagedCreate((UpdateTableRequest) body);
  }

  /**
   * Whether the commit is a staged create, identified (as in Iceberg's reference {@code
   * CatalogHandlers}) by an {@code assert-create} ({@code AssertTableDoesNotExist}) requirement.
   * Shared by this extractor and the handler's dispatch so the two cannot disagree on which branch
   * a commit takes.
   */
  public static boolean isStagedCreate(UpdateTableRequest request) {
    return request.requirements().stream()
        .anyMatch(r -> r instanceof UpdateRequirement.AssertTableDoesNotExist);
  }
}
