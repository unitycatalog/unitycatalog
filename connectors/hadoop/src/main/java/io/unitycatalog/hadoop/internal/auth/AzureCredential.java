package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.hadoop.internal.CredentialUtil;
import java.util.Objects;

public final class AzureCredential extends GenericCredential {
  private final String sasToken;

  public AzureCredential(String sasToken, Long expirationTimeMillis) {
    super(expirationTimeMillis);
    this.sasToken = CredentialUtil.field(sasToken, "Azure SAS token is missing");
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
