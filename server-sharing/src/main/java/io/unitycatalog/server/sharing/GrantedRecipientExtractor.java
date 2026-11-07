package io.unitycatalog.server.sharing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.opensharing.http.ApiException;
import io.opensharing.http.ApiJson;
import io.opensharing.share.UpdateSharePermissionsRequest;
import io.unitycatalog.server.auth.decorator.AuthorizeValueExtractor;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The recipient a {@code PATCH /shares/{share}/permissions} body grants to, or null for none. A
 * resource key names one recipient, so a body that grants to more than one is refused. It reads the
 * body as OpenSharing does, so it names the recipient OpenSharing would grant to. A body
 * OpenSharing can't read, and a change it rejects, name none: OpenSharing checks every change
 * before applying any.
 */
public final class GrantedRecipientExtractor implements AuthorizeValueExtractor {

  @Override
  public Object extract(Object body) {
    if (!(body instanceof JsonNode json) || !json.isObject()) {
      return null;
    }
    UpdateSharePermissionsRequest request;
    try {
      request = ApiJson.MAPPER.treeToValue(json, UpdateSharePermissionsRequest.class);
    } catch (JsonProcessingException e) {
      return null;
    }
    Set<String> granted = new LinkedHashSet<>();
    for (UpdateSharePermissionsRequest.Change change : request.changes()) {
      if (change != null
          && !change.add().isEmpty()
          && change.recipientName() != null
          && !change.recipientName().isBlank()) {
        granted.add(change.recipientName());
      }
    }
    if (granted.size() > 1) {
      throw ApiException.invalidParameter("a request can grant a share to only one recipient");
    }
    return granted.isEmpty() ? null : granted.iterator().next();
  }
}
