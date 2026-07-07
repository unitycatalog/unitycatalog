package io.unitycatalog.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.unitycatalog.client.ApiException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructField;

/**
 * Shared helpers for converting Spark schema/partitioning into the wire shapes Unity Catalog
 * expects when creating a table. Extracted from {@code UCSingleCatalog}/{@code UCProxy} so the
 * single table-create path has one implementation (and so the view-create path can reuse the same
 * serialization instead of duplicating it).
 */
public final class UCColumnConversions {

  private UCColumnConversions() {}

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Serialize a Spark {@link StructField} to the canonical "StructField-shape" JSON that Unity
   * Catalog stores in {@code ColumnInfo.type_json}: {@code {name, type, nullable, metadata}}.
   * Matches the shape produced by {@code StructField.toJson}. The field's {@code metadata} (which
   * on a {@code StructField} already carries the column comment under the {@code "comment"} key) is
   * emitted verbatim.
   */
  public static String toStructFieldJson(StructField field) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("name", field.name());
    // dataType().json() / metadata().json() are already JSON documents; embed them as parsed nodes
    // so they are not re-escaped as strings.
    node.set("type", readTree(field.dataType().json()));
    node.put("nullable", field.nullable());
    node.set("metadata", readTree(field.metadata().json()));
    return writeValueAsString(node);
  }

  /**
   * Resolve the partition column names from Spark's V2 partition {@link Transform}s.
   *
   * <ul>
   *   <li>{@code identity(col)} is a real partition column; UC's wire model records partition
   *       columns by name, so we take the referenced field name. An identity transform references
   *       exactly one field, hence the single-field check.
   *   <li>{@code cluster_by(...)} is liquid clustering, not partitioning. UC's create-table wire
   *       model has no clustering concept, so these transforms contribute no partition columns
   *       (dropped).
   *   <li>Anything else is not representable and is rejected.
   * </ul>
   */
  public static List<String> partitionColumnNames(Transform[] partitions) throws ApiException {
    List<String> names = new ArrayList<>();
    if (partitions == null) {
      return names;
    }
    for (Transform t : partitions) {
      switch (t.name()) {
        case "identity":
          List<String> fieldNames = new ArrayList<>();
          for (NamedReference ref : t.references()) {
            for (String part : ref.fieldNames()) {
              fieldNames.add(part);
            }
          }
          if (fieldNames.size() != 1) {
            throw new IllegalArgumentException(
                "Expected single-field partition reference but got: "
                    + String.join(".", fieldNames));
          }
          names.add(fieldNames.get(0));
          break;
        case "cluster_by":
          break;
        default:
          throw new ApiException("Unsupported partition transform: " + t.name());
      }
    }
    return names;
  }

  /** {@link ObjectMapper#readTree(String)} with the checked exception rethrown as unchecked. */
  private static JsonNode readTree(String json) {
    try {
      return MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Malformed column JSON: " + e.getMessage(), e);
    }
  }

  /** {@link ObjectMapper#writeValueAsString} with the checked exception rethrown as unchecked. */
  private static String writeValueAsString(JsonNode node) {
    try {
      return MAPPER.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      // Serializing an in-memory node should never fail.
      throw new IllegalStateException("Failed to serialize column JSON", e);
    }
  }
}
