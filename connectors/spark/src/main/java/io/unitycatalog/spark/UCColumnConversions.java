package io.unitycatalog.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.internal.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataType;
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

  // Field names of the canonical "StructField-shape" JSON stored in ColumnInfo.type_json.
  private static final String FIELD_NAME = "name";
  private static final String FIELD_TYPE = "type";
  private static final String FIELD_NULLABLE = "nullable";
  private static final String FIELD_METADATA = "metadata";
  private static final String FIELD_COMMENT = "comment";

  /**
   * Serialize a Spark {@link StructField} to the canonical "StructField-shape" JSON that Unity
   * Catalog stores in {@code ColumnInfo.type_json}: {@code {name, type, nullable, metadata}}.
   * Matches the shape produced by {@code StructField.toJson}. The field's {@code metadata} (which
   * on a {@code StructField} already carries the column comment under the {@code "comment"} key) is
   * emitted verbatim.
   */
  public static String toStructFieldJson(StructField field) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put(FIELD_NAME, field.name());
    // dataType().json() / metadata().json() are already JSON documents; embed them as parsed nodes
    // so they are not re-escaped as strings.
    node.set(FIELD_TYPE, readTree(field.dataType().json()));
    node.put(FIELD_NULLABLE, field.nullable());
    node.set(FIELD_METADATA, readTree(field.metadata().json()));
    return writeValueAsString(node);
  }

  /**
   * Spark V2 {@link Column} -&gt; wire: build the same {@code {name, type, nullable, metadata}}
   * JSON as {@link #toStructFieldJson} but from a V2 catalog {@code Column}. Unlike a {@code
   * StructField} (whose {@code metadata} already contains any comment), a V2 {@code Column} exposes
   * its comment via the dedicated {@code Column.comment()} accessor, so the comment is merged back
   * under the {@code "comment"} metadata key here to keep the wire shape identical.
   */
  public static String buildColumnJson(Column col) {
    // metadataInJSON() is nullable for fields without analyzer-attached metadata.
    String metaJson = col.metadataInJSON();
    ObjectNode metadataNode =
        (metaJson == null) ? MAPPER.createObjectNode() : (ObjectNode) readTree(metaJson);
    // Column.comment() is nullable; when present, merge it under "comment".
    if (col.comment() != null) {
      metadataNode.put(FIELD_COMMENT, col.comment());
    }
    ObjectNode node = MAPPER.createObjectNode();
    node.put(FIELD_NAME, col.name());
    node.set(FIELD_TYPE, readTree(col.dataType().json()));
    node.put(FIELD_NULLABLE, col.nullable());
    node.set(FIELD_METADATA, metadataNode);
    return writeValueAsString(node);
  }

  /**
   * Wire -&gt; Spark V2 {@link Column}: parse a {@code StructField}-shape JSON string back into a
   * {@code Column}. The {@code "comment"} key (if present in the wire metadata) is lifted out and
   * passed to {@code Column.create} as the dedicated {@code comment} arg, NOT left in {@code
   * metadataInJSON} -- matches what {@code CatalogV2Util.structFieldToV2Column} does on the v1
   * path.
   */
  public static Column parseColumnJson(String jsonStr) {
    // `ColumnInfo.type_json` is nullable on the wire (e.g. a view-like row created by an older
    // writer or a non-Spark client that did not round-trip the field). Fail with a clear,
    // actionable message rather than an opaque NPE deeper in the view-load path.
    Preconditions.checkNotNull(
        jsonStr, "Column type_json is missing; cannot reconstruct the Spark column for this view");
    JsonNode parsed = readTree(jsonStr);
    JsonNode nameNode = parsed.get(FIELD_NAME);
    Preconditions.checkArgument(
        nameNode != null && nameNode.isTextual(),
        "Expected string `name` in StructField JSON, got: %s",
        nameNode);
    String name = nameNode.asText();

    // Re-serialize the type sub-node to a compact string for DataType.fromJson.
    DataType dataType = DataType.fromJson(writeValueAsString(parsed.get(FIELD_TYPE)));

    JsonNode nullableNode = parsed.get(FIELD_NULLABLE);
    Preconditions.checkArgument(
        nullableNode != null && nullableNode.isBoolean(),
        "Expected boolean `nullable` in StructField JSON, got: %s",
        nullableNode);
    boolean nullable = nullableNode.asBoolean();

    JsonNode metadataNode = parsed.get(FIELD_METADATA);
    ObjectNode metadataObj =
        (metadataNode != null && metadataNode.isObject())
            ? (ObjectNode) metadataNode.deepCopy()
            : MAPPER.createObjectNode();
    // Lift "comment" out of metadata and pass it as the dedicated Column.create arg.
    JsonNode commentNode = metadataObj.remove(FIELD_COMMENT);
    String comment = (commentNode != null && commentNode.isTextual()) ? commentNode.asText() : null;
    // Empty leftover metadata maps back to null (not "{}"), matching the StructField wire form.
    String metadataInJSON = metadataObj.isEmpty() ? null : writeValueAsString(metadataObj);

    return Column.create(name, dataType, nullable, comment, metadataInJSON);
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
