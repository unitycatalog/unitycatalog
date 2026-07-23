package io.unitycatalog.hadoop.internal.id;

import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.net.URI;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;

/**
 * Uniquely identifies a credential-scoped delegate filesystem, so the correct delegate can be
 * selected and reused from the cache. A delegate covers exactly one credential and storage prefix,
 * and this id pairs the two dimensions that pin it down:
 *
 * <ul>
 *   <li>{@link CredId} — the credential scope, used to retrieve the vended credential.
 *   <li>{@code location} — the storage prefix the credential covers, which distinguishes delegates
 *       when a single credential scope vends several prefix-scoped credentials.
 * </ul>
 *
 * <p>{@code location} is nullable: a {@code null} location keys purely by {@link CredId}.
 *
 * <p><b>Internal API — not for external use. May change without notice.</b>
 */
public final class DelegateFileSystemId {
  private final CredId credId;
  private final String location;

  private DelegateFileSystemId(CredId credId, String location) {
    Preconditions.checkNotNull(credId, "credId is required");
    this.credId = credId;
    this.location = location;
  }

  /**
   * Pairs {@code credId} with the {@code location} being accessed ({@code null} keys by CredId
   * only).
   */
  public static DelegateFileSystemId of(CredId credId, String location) {
    return new DelegateFileSystemId(credId, location);
  }

  /**
   * Derives the id from {@code conf}: the {@link CredId} for the credential scope plus the {@link
   * UCHadoopConfConstants#UC_CREDENTIAL_LOCATION_KEY location} being served.
   */
  public static DelegateFileSystemId create(Configuration conf) {
    return of(CredId.create(conf), location(conf));
  }

  /**
   * Like {@link #create(Configuration)} but for a filesystem being initialized on {@code uri}: when
   * the configuration carries no Unity Catalog credential type, falls back to a {@link
   * DefaultCredId} derived from the URI's scheme and authority.
   */
  public static DelegateFileSystemId create(Configuration conf, URI uri) {
    return of(CredId.create(conf, () -> new DefaultCredId(uri, conf)), location(conf));
  }

  private static String location(Configuration conf) {
    return conf.get(UCHadoopConfConstants.UC_CREDENTIAL_LOCATION_KEY);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DelegateFileSystemId)) {
      return false;
    }
    DelegateFileSystemId that = (DelegateFileSystemId) o;
    return Objects.equals(credId, that.credId) && Objects.equals(location, that.location);
  }

  @Override
  public int hashCode() {
    return Objects.hash(credId, location);
  }

  @Override
  public String toString() {
    return "DelegateFileSystemId{credId=" + credId + ", location=" + location + "}";
  }
}
