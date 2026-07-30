package io.unitycatalog.hadoop.internal.id;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.net.URI;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;

/**
 * Uniquely identifies a concrete Hadoop filesystem. Hadoop's canonical filesystem API supports one
 * credential for the entire filesystem, while each credential returned by Unity Catalog is scoped
 * to a storage prefix. Both the credential request ({@link CredId}) and prefix are therefore
 * required to identify the filesystem.
 *
 * <p>{@code prefix} is nullable: a {@code null} prefix keys purely by {@link CredId}.
 *
 * <p><b>Internal API — not for external use. May change without notice.</b>
 */
public final class FileSystemCredId {
  private final CredId credId;
  private final String prefix;

  private FileSystemCredId(CredId credId, String prefix) {
    this.credId = credId;
    this.prefix = prefix;
  }

  /**
   * Derives the id from {@code conf}: the {@link CredId} for the credential request plus the {@link
   * UCHadoopConfConstants#UC_CREDENTIAL_PREFIX_KEY prefix} being served.
   */
  public static FileSystemCredId create(Configuration conf) {
    CredId credId = CredId.create(conf);
    String prefix = getCredPrefix(conf);
    return new FileSystemCredId(credId, prefix);
  }

  /**
   * Like {@link #create(Configuration)} but for a filesystem being initialized on {@code uri}: when
   * the configuration carries no Unity Catalog credential type, falls back to a {@link
   * DefaultCredId} derived from the URI's scheme and authority. Reads the prefix from a namespaced
   * key when the selected credential's config is prefixed with {@code namespace} (multi-credential
   * layout); a {@code null} namespace reads the top-level {@link
   * UCHadoopConfConstants#UC_CREDENTIAL_PREFIX_KEY prefix} key.
   */
  public static FileSystemCredId create(Configuration conf, URI uri, String namespace) {
    CredId credId = CredId.create(conf, () -> new DefaultCredId(uri, conf));
    String prefix = getCredPrefix(conf, namespace);
    return new FileSystemCredId(credId, prefix);
  }

  private static String getCredPrefix(Configuration conf) {
    return conf.get(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY);
  }

  private static String getCredPrefix(Configuration conf, String namespace) {
    String prefixKey =
        namespace == null
            ? UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY
            : namespace + UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY;
    return conf.get(prefixKey);
  }

  /**
   * The credential prefix being accessed, or {@code null} when the key is scoped by CredId only.
   */
  public String prefix() {
    return prefix;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FileSystemCredId)) {
      return false;
    }
    FileSystemCredId that = (FileSystemCredId) o;
    return Objects.equals(credId, that.credId) && Objects.equals(prefix, that.prefix);
  }

  @Override
  public int hashCode() {
    return Objects.hash(credId, prefix);
  }

  @Override
  public String toString() {
    return "FileSystemCredId{credId=" + credId + ", prefix=" + prefix + "}";
  }
}
