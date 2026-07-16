package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.internal.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** An immutable list of {@link GenericCredential}s. */
public final class CredentialBundle {
  private final List<GenericCredential> credentials;

  private CredentialBundle(List<GenericCredential> credentials) {
    this.credentials = credentials;
  }

  public static CredentialBundle of(List<GenericCredential> creds) {
    Preconditions.checkNotNull(creds, "credentials is required");
    Preconditions.checkArgument(!creds.isEmpty(), "credentials must not be empty");
    for (GenericCredential cred : creds) {
      Preconditions.checkNotNull(cred, "credentials must not contain null");
    }
    return new CredentialBundle(Collections.unmodifiableList(new ArrayList<>(creds)));
  }

  /** Creates a bundle holding a single credential. */
  public static CredentialBundle of(GenericCredential cred) {
    Preconditions.checkNotNull(cred, "credential is required");
    return new CredentialBundle(Collections.singletonList(cred));
  }

  /** All credentials in the batch, in fetch order. Never empty. */
  public List<GenericCredential> all() {
    return credentials;
  }

  /**
   * Returns the sole credential.
   *
   * @throws IllegalArgumentException if the batch holds more than one credential.
   */
  public GenericCredential single() {
    Preconditions.checkArgument(
        credentials.size() == 1,
        "Expected exactly one credential but found %s.",
        credentials.size());
    return credentials.get(0);
  }

  public boolean isEmpty() {
    return credentials.isEmpty();
  }
}
