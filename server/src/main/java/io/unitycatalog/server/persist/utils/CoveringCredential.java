package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.persist.dao.CredentialDAO;

/** The storage credential of the external location that covers a path. */
public sealed interface CoveringCredential {

  /** No external location covers the path. */
  record NotCovered() implements CoveringCredential {}

  record Covered(CredentialDAO credential) implements CoveringCredential {}

  /** The covering external location's storage credential was force-deleted. */
  record Deleted() implements CoveringCredential {}
}
