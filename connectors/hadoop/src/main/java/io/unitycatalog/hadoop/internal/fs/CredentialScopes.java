package io.unitycatalog.hadoop.internal.fs;

import io.unitycatalog.hadoop.internal.DeltaStorageCredentialUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * Encodes and applies multi-location credential scopes carried in a Hadoop configuration.
 *
 * <p>A table whose reads span locations beyond its own carries one scope per extra vended
 * credential: the location prefix it covers plus a complete set of credential properties for that
 * prefix. {@link #selectConf} overlays the covering scope's properties — longest covering prefix
 * wins — so {@link CredScopedFileSystem} creates the delegate for those paths with that scope's
 * credentials. The selection is prefix-based, not bucket-based, so scopes that share a bucket are
 * supported, and the overlaid properties may target any cloud scheme.
 */
public final class CredentialScopes {

  private CredentialScopes() {}

  /** Writes scope {@code index} ({@code prefix} plus its credential {@code props}) into a map. */
  public static void encode(
      Map<String, String> into, int index, String prefix, Map<String, String> props) {
    String ns = UCHadoopConfConstants.UC_CRED_SCOPE_PREFIX + index;
    into.put(ns + UCHadoopConfConstants.UC_CRED_SCOPE_PREFIX_SUFFIX, prefix);
    for (Map.Entry<String, String> e : props.entrySet()) {
      into.put(ns + UCHadoopConfConstants.UC_CRED_SCOPE_PROP_SUFFIX + e.getKey(), e.getValue());
    }
  }

  /**
   * Returns {@code conf} with the credential properties of the scope covering {@code uri} overlaid,
   * or {@code conf} unchanged when no scope covers it (the table's own credentials already apply).
   */
  public static Configuration selectConf(URI uri, Configuration conf) {
    int count = conf.getInt(UCHadoopConfConstants.UC_CRED_SCOPE_COUNT_KEY, 0);
    if (count <= 0 || uri == null) {
      return conf;
    }
    String path = uri.toString();
    int best = -1;
    int bestLength = -1;
    for (int i = 0; i < count; i++) {
      String prefix =
          conf.get(
              UCHadoopConfConstants.UC_CRED_SCOPE_PREFIX
                  + i
                  + UCHadoopConfConstants.UC_CRED_SCOPE_PREFIX_SUFFIX);
      if (prefix == null || !DeltaStorageCredentialUtil.prefixCovers(path, prefix)) {
        continue;
      }
      if (prefix.length() > bestLength) {
        best = i;
        bestLength = prefix.length();
      }
    }
    if (best < 0) {
      return conf;
    }
    String propNamespace =
        UCHadoopConfConstants.UC_CRED_SCOPE_PREFIX
            + best
            + UCHadoopConfConstants.UC_CRED_SCOPE_PROP_SUFFIX;
    Configuration effective = new Configuration(conf);
    for (Map.Entry<String, String> entry : conf.getPropsWithPrefix(propNamespace).entrySet()) {
      effective.set(entry.getKey(), entry.getValue());
    }
    return effective;
  }
}
