package io.unitycatalog.hadoop.internal.auth;

import java.util.Objects;

public final class GcsCredential extends GenericCredential {
  private final String oauthToken;

  public GcsCredential(String oauthToken, Long expirationTimeMillis) {
    super(expirationTimeMillis);
    this.oauthToken = oauthToken;
  }

  public String oauthToken() {
    return oauthToken;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GcsCredential)) {
      return false;
    }
    GcsCredential that = (GcsCredential) o;
    return Objects.equals(oauthToken, that.oauthToken)
        && Objects.equals(expirationTimeMillis(), that.expirationTimeMillis());
  }

  @Override
  public int hashCode() {
    return Objects.hash(oauthToken, expirationTimeMillis());
  }
}
