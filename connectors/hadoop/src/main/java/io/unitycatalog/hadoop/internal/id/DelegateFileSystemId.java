package io.unitycatalog.hadoop.internal.id;

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
 *   <li>{@code prefix} — the storage prefix the credential covers, which distinguishes delegates
 *       when a single credential scope vends several prefix-scoped credentials.
 * </ul>
 *
 * <p>{@code prefix} is nullable: a {@code null} prefix keys purely by {@link CredId}.
 *
 * <p><b>Internal API — not for external use. May change without notice.</b>
 */
public final class DelegateFileSystemId {
  private final CredId credId;
  private final String prefix;

  private DelegateFileSystemId(CredId credId, String prefix) {
    this.credId = credId;
    this.prefix = prefix;
  }

  /**
   * Derives the id from {@code conf}: the {@link CredId} for the credential scope plus the {@link
   * UCHadoopConfConstants#UC_CREDENTIAL_PREFIX_KEY prefix} being served.
   */
  public static DelegateFileSystemId create(Configuration conf) {
    return new DelegateFileSystemId(CredId.create(conf), prefix(conf));
  }

  /**
   * Like {@link #create(Configuration)} but for a filesystem being initialized on {@code uri}: when
   * the configuration carries no Unity Catalog credential type, falls back to a {@link
   * DefaultCredId} derived from the URI's scheme and authority.
   */
  public static DelegateFileSystemId create(Configuration conf, URI uri) {
    return new DelegateFileSystemId(
        CredId.create(conf, () -> new DefaultCredId(uri, conf)), prefix(conf));
  }

  private static String prefix(Configuration conf) {
    return conf.get(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY);
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
    return Objects.equals(credId, that.credId) && Objects.equals(prefix, that.prefix);
  }

  @Override
  public int hashCode() {
    return Objects.hash(credId, prefix);
  }

  @Override
  public String toString() {
    return "DelegateFileSystemId{credId=" + credId + ", prefix=" + prefix + "}";
  }
}
