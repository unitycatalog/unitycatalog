package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.internal.Clock;

/**
 * Internal credential model used by Hadoop token providers. Each instance contains only one set of
 * credentials for a single cloud family.
 */
public abstract class GenericCredential {
  private final Long expirationTimeMillis;

  protected GenericCredential(Long expirationTimeMillis) {
    this.expirationTimeMillis = expirationTimeMillis;
  }

  public final Long expirationTimeMillis() {
    return expirationTimeMillis;
  }

  public final boolean readyToRenew(Clock clock, long renewalLeadTimeMillis) {
    return expirationTimeMillis != null
        && expirationTimeMillis <= clock.now().toEpochMilli() + renewalLeadTimeMillis;
  }
}
