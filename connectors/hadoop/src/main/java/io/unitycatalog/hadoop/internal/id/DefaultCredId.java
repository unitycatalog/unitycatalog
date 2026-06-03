package io.unitycatalog.hadoop.internal.id;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * {@link CredId} keyed by URI scheme and authority; used as a fallback when no Unity Catalog
 * credential type is present in the configuration.
 */
public class DefaultCredId implements CredId {
  private final String scheme;
  private final String authority;

  public DefaultCredId(URI uri, Configuration conf) {
    if (uri.getScheme() == null && uri.getAuthority() == null) {
      URI defaultUri = FileSystem.getDefaultUri(conf);
      this.scheme = defaultUri.getScheme();
      this.authority = defaultUri.getAuthority();
    } else {
      this.scheme = uri.getScheme();
      this.authority = uri.getAuthority();
    }
  }

  @Override
  public Map<String, String> props() {
    // The fallback scope is identified by URI scheme + authority and carries no Unity Catalog
    // credential-request properties, so there is nothing to emit.
    return Collections.emptyMap();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DefaultCredId)) return false;
    DefaultCredId that = (DefaultCredId) o;
    return Objects.equals(scheme, that.scheme) && Objects.equals(authority, that.authority);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scheme, authority);
  }

  @Override
  public String toString() {
    return "DefaultCredId{scheme=" + scheme + ", authority=" + authority + "}";
  }
}
