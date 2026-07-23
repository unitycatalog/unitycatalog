package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.internal.Clock;
import java.util.Objects;

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

  /** Subclasses chain via {@code super.equals} to compare the shared expiration. */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GenericCredential)) {
      return false;
    }
    return Objects.equals(expirationTimeMillis, ((GenericCredential) o).expirationTimeMillis);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(expirationTimeMillis);
  }
}
