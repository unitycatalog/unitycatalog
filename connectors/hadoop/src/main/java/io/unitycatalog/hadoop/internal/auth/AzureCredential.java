package io.unitycatalog.hadoop.internal.auth;

import java.util.Objects;

public final class AzureCredential extends GenericCredential {
  private final String sasToken;

  public AzureCredential(String sasToken, Long expirationTimeMillis) {
    super(expirationTimeMillis);
    this.sasToken = sasToken;
  }

  public String sasToken() {
    return sasToken;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AzureCredential)) {
      return false;
    }
    AzureCredential that = (AzureCredential) o;
    return Objects.equals(sasToken, that.sasToken)
        && Objects.equals(expirationTimeMillis(), that.expirationTimeMillis());
  }

  @Override
  public int hashCode() {
    return Objects.hash(sasToken, expirationTimeMillis());
  }
}
