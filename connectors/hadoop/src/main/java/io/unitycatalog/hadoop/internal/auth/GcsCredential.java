package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.hadoop.internal.CredentialUtil;
import java.util.Objects;

public final class GcsCredential extends GenericCredential {
  private final String oauthToken;

  public GcsCredential(String oauthToken, Long expirationTimeMillis) {
    super(expirationTimeMillis);
    this.oauthToken = CredentialUtil.field(oauthToken, "GCS OAuth token is missing");
  }

  public String oauthToken() {
    return oauthToken;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GcsCredential) || !super.equals(o)) {
      return false;
    }
    GcsCredential that = (GcsCredential) o;
    return Objects.equals(oauthToken, that.oauthToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), oauthToken);
  }
}
