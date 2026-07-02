package io.unitycatalog.hadoop.internal.id;

import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_CATALOG_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_DEFAULT_VALUE;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_LOCATION_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_PATH_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_PATH_OPERATION_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_TABLE_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_TABLE_OPERATION_KEY;

import io.unitycatalog.hadoop.internal.UCDeltaTableIdentifier;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;

/**
 * Identifies a credential scope: the resource a Unity Catalog vended credential grants access to.
 *
 * <p>Two requests sharing the same {@code CredId} target the same credential scope and can reuse a
 * vended credential, while requests with different ids are isolated.
 *
 * <p>There are five implementations:
 *
 * <ul>
 *   <li>{@link TableCredId} — keyed by table ID and operation; used for table-level temporary
 *       credentials via the UC credentials API.
 *   <li>{@link PathCredId} — keyed by path and operation; used for path-level temporary
 *       credentials.
 *   <li>{@link DeltaTableCredId} — keyed by table identity, operation, and location; used for
 *       table-level temporary credentials via the UC Delta credentials API.
 *   <li>{@link DeltaStagingTableCredId} — keyed by staging table ID and location; used for
 *       staging-table-level temporary credentials via the UC Delta credentials API.
 *   <li>{@link DefaultCredId} — keyed by URI scheme and authority; used as a fallback when no Unity
 *       Catalog credential type is present in the configuration.
 * </ul>
 */
public interface CredId {

  static CredId create(Configuration conf) {
    return create(
        conf,
        () -> {
          Map<String, String> observed = new LinkedHashMap<>();
          observed.put(UC_CREDENTIALS_TYPE_KEY, conf.get(UC_CREDENTIALS_TYPE_KEY));
          observed.put(
              UC_DELTA_CREDENTIALS_API_ENABLED_KEY, conf.get(UC_DELTA_CREDENTIALS_API_ENABLED_KEY));
          observed.put(UC_DELTA_STAGING_TABLE_ID_KEY, conf.get(UC_DELTA_STAGING_TABLE_ID_KEY));

          throw new IllegalArgumentException(
              "Cannot recognize the hadoop config to initialize the CredId. Observed: " + observed);
        });
  }

  static CredId create(Configuration conf, Supplier<CredId> defaultCredId) {
    String type = conf.get(UC_CREDENTIALS_TYPE_KEY);
    boolean isDeltaApi =
        conf.getBoolean(
            UC_DELTA_CREDENTIALS_API_ENABLED_KEY, UC_DELTA_CREDENTIALS_API_ENABLED_DEFAULT_VALUE);
    String stagingTableId = conf.get(UC_DELTA_STAGING_TABLE_ID_KEY);
    // stagingTableId is always from UC Delta API.
    boolean hasUcDeltaStagingTableId = stagingTableId != null && !stagingTableId.isEmpty();

    if (UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type) && !isDeltaApi && !hasUcDeltaStagingTableId) {
      // Case 1: UC table (legacy API) — keyed by table ID + operation.
      String tableOp = conf.get(UC_TABLE_OPERATION_KEY);
      String tableId = conf.get(UC_TABLE_ID_KEY);
      return new TableCredId(tableId, tableOp);

    } else if (UC_CREDENTIALS_TYPE_PATH_VALUE.equals(type)
        && !isDeltaApi
        && !hasUcDeltaStagingTableId) {
      // Case 2: Path-based credentials (legacy UC API) — keyed by path + operation.
      String path = conf.get(UC_PATH_KEY);
      String pathOp = conf.get(UC_PATH_OPERATION_KEY);
      return new PathCredId(path, pathOp);

    } else if (UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type)
        && isDeltaApi
        && !hasUcDeltaStagingTableId) {
      // Case 3: Delta table — keyed by catalog.schema.table + operation + location.
      String tableOp = conf.get(UC_TABLE_OPERATION_KEY);
      UCDeltaTableIdentifier identifier =
          UCDeltaTableIdentifier.of(
              conf.get(UC_DELTA_CATALOG_KEY),
              conf.get(UC_DELTA_SCHEMA_KEY),
              conf.get(UC_DELTA_TABLE_NAME_KEY));
      String location = conf.get(UC_DELTA_LOCATION_KEY);
      return new DeltaTableCredId(identifier, tableOp, location);

    } else if (hasUcDeltaStagingTableId) {
      // Case 4: Delta staging table — keyed by staging table UUID + location.
      String location = conf.get(UC_DELTA_STAGING_TABLE_LOCATION_KEY);
      return new DeltaStagingTableCredId(stagingTableId, location);

    } else {
      // Case 5: No recognized credential type — delegate to the caller-provided fallback.
      return defaultCredId.get();
    }
  }

  /**
   * Generate the CredId's corresponding hadoop config properties. The properties returned here can
   * be used to reconstruct the CredId via {@link #create(Configuration)}.
   *
   * @return an unmodifiable map to represent the config key and values; empty when the scope
   *     carries no Unity Catalog credential-request properties (e.g. {@link DefaultCredId}).
   */
  Map<String, String> props();
}
