package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.model.TemporaryCredentials;

/** A vended credential scoped to a storage location prefix other than the table's own. */
public final class ScopedCredential {
  private final String prefix;
  private final String operation;
  private final TemporaryCredentials credentials;

  public ScopedCredential(String prefix, String operation, TemporaryCredentials credentials) {
    this.prefix = prefix;
    this.operation = operation;
    this.credentials = credentials;
  }

  /** The location prefix this credential covers. */
  public String prefix() {
    return prefix;
  }

  /** The wire value of the operation this credential is scoped to (e.g. {@code READ}). */
  public String operation() {
    return operation;
  }

  public TemporaryCredentials credentials() {
    return credentials;
  }
}
