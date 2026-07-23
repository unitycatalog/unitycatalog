package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Objects;

public final class AzureCredential extends GenericCredential {
  private final String sasToken;

  public AzureCredential(String sasToken, Long expirationTimeMillis, String location) {
    super(expirationTimeMillis, location);
    Preconditions.checkArgument(
        sasToken != null && !sasToken.isEmpty(), "Azure SAS token is missing");
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
    if (!(o instanceof AzureCredential) || !super.equals(o)) {
      return false;
    }
    AzureCredential that = (AzureCredential) o;
    return Objects.equals(sasToken, that.sasToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), sasToken);
  }
}
