package io.unitycatalog.server.service.iceberg;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import java.util.Map;
import org.apache.iceberg.rest.responses.LoadTableResponse;

/**
 * Wraps a standard LoadTableResponse with an optional labels field for catalog metadata
 * enrichment. Labels are ephemeral API-level annotations — not part of table state.
 *
 * <p>This is a proof-of-concept for the IRC Labels proposal. Labels are derived from UC entity
 * properties (prefix "label."), which are catalog-scoped and separate from Iceberg table metadata.
 */
public class LoadTableResponseWithLabels {

  @JsonUnwrapped
  private final LoadTableResponse delegate;

  private final Labels labels;

  public LoadTableResponseWithLabels(LoadTableResponse delegate, Labels labels) {
    this.delegate = delegate;
    this.labels = labels;
  }

  public LoadTableResponse getDelegate() {
    return delegate;
  }

  public Labels getLabels() {
    return labels;
  }

  public static class Labels {
    private final Map<String, String> table;

    public Labels(Map<String, String> table) {
      this.table = table;
    }

    public Map<String, String> getTable() {
      return table;
    }
  }
}
