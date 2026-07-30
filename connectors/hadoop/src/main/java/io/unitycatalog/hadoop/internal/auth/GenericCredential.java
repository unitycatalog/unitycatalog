package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.internal.Clock;
import java.util.Objects;

/**
 * Internal credential model used by Hadoop token providers. Each instance contains only one set of
 * credentials for a single cloud family.
 */
public abstract class GenericCredential {
  private final Long expirationTimeMillis;
  private final String prefix;

  protected GenericCredential(Long expirationTimeMillis, String prefix) {
    this.expirationTimeMillis = expirationTimeMillis;
    this.prefix = prefix;
  }

  public final Long expirationTimeMillis() {
    return expirationTimeMillis;
  }

  /** The storage prefix this credential is scoped to, or {@code null} when not provided. */
  public final String prefix() {
    return prefix;
  }

  public final boolean readyToRenew(Clock clock, long renewalLeadTimeMillis) {
    return expirationTimeMillis != null
        && expirationTimeMillis <= clock.now().toEpochMilli() + renewalLeadTimeMillis;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GenericCredential)) {
      return false;
    }
    GenericCredential that = (GenericCredential) o;
    return Objects.equals(expirationTimeMillis, that.expirationTimeMillis)
        && Objects.equals(prefix, that.prefix);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expirationTimeMillis, prefix);
  }
}
