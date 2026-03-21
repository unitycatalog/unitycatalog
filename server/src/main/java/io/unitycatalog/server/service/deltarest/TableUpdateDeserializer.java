package io.unitycatalog.server.service.deltarest;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.unitycatalog.server.model.deltarest.AddCommitUpdate;
import io.unitycatalog.server.model.deltarest.RemoveDomainMetadataUpdate;
import io.unitycatalog.server.model.deltarest.RemovePropertiesUpdate;
import io.unitycatalog.server.model.deltarest.SetColumnsUpdate;
import io.unitycatalog.server.model.deltarest.SetDomainMetadataUpdate;
import io.unitycatalog.server.model.deltarest.SetLatestBackfilledVersionUpdate;
import io.unitycatalog.server.model.deltarest.SetPartitionColumnsUpdate;
import io.unitycatalog.server.model.deltarest.SetPropertiesUpdate;
import io.unitycatalog.server.model.deltarest.SetProtocolUpdate;
import io.unitycatalog.server.model.deltarest.SetTableCommentUpdate;
import io.unitycatalog.server.model.deltarest.UpdateSnapshotVersionUpdate;
import java.io.IOException;

/**
 * Custom Jackson deserializer for the polymorphic TableUpdate type.
 * Dispatches based on the "action" field.
 */
public class TableUpdateDeserializer
    extends StdDeserializer<Object> {

  public TableUpdateDeserializer() {
    super(Object.class);
  }

  @Override
  public Object deserialize(
      JsonParser p, DeserializationContext ctxt) throws IOException {
    JsonNode node = p.getCodec().readTree(p);
    String action =
        node.has("action") ? node.get("action").asText() : null;
    if (action == null) {
      throw new IOException(
          "TableUpdate missing required 'action' field");
    }

    return switch (action) {
      case "set-properties" ->
          p.getCodec().treeToValue(node, SetPropertiesUpdate.class);
      case "remove-properties" ->
          p.getCodec().treeToValue(
              node, RemovePropertiesUpdate.class);
      case "set-protocol" ->
          p.getCodec().treeToValue(node, SetProtocolUpdate.class);
      case "set-domain-metadata" ->
          p.getCodec().treeToValue(
              node, SetDomainMetadataUpdate.class);
      case "remove-domain-metadata" ->
          p.getCodec().treeToValue(
              node, RemoveDomainMetadataUpdate.class);
      case "set-columns" ->
          p.getCodec().treeToValue(node, SetColumnsUpdate.class);
      case "set-partition-columns" ->
          p.getCodec().treeToValue(
              node, SetPartitionColumnsUpdate.class);
      case "set-table-comment" ->
          p.getCodec().treeToValue(
              node, SetTableCommentUpdate.class);
      case "add-commit" ->
          p.getCodec().treeToValue(node, AddCommitUpdate.class);
      case "set-latest-backfilled-version" ->
          p.getCodec().treeToValue(
              node, SetLatestBackfilledVersionUpdate.class);
      case "update-snapshot-version" ->
          p.getCodec().treeToValue(
              node, UpdateSnapshotVersionUpdate.class);
      default ->
          throw new IOException(
              "Unknown TableUpdate action: " + action);
    };
  }
}
