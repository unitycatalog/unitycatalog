package io.unitycatalog.server.service.sharing;

import io.opensharing.http.ApiException;
import io.opensharing.share.UpdateSharePermissionsRequest;
import io.unitycatalog.server.auth.decorator.AuthorizeValueExtractor;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The recipient a {@code PATCH /shares/{share}/permissions} body grants to, or null for none. A
 * resource key names one recipient, so a body that grants to more than one is refused. A change
 * that only revokes names none, and OpenSharing rejects an invalid change itself: it checks every
 * change before applying any.
 */
public final class GrantedRecipientExtractor implements AuthorizeValueExtractor {

  @Override
  public Object extract(Object body) {
    Set<String> granted = new LinkedHashSet<>();
    for (UpdateSharePermissionsRequest.Change change :
        ((UpdateSharePermissionsRequest) body).changes()) {
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
