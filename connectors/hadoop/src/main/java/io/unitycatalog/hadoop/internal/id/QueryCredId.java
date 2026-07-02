package io.unitycatalog.hadoop.internal.id;

import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_UID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIAL_CACHE_SCOPE_DEFAULT_VALUE;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIAL_CACHE_SCOPE_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIAL_CACHE_SCOPE_QUERY;

import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs.CredentialCacheScope;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;

/**
 * Per-query credential cache identity.
 *
 * <p>Used only when credential cache scope is {@code query}. Cluster-scoped caching continues to
 * key entries by the resource {@link CredId} alone.
 */
public final class QueryCredId implements CredId {
  private final String uuid;

  public QueryCredId(String uuid) {
    Preconditions.checkNotNull(uuid, "uuid is required");
    Preconditions.checkArgument(!uuid.isEmpty(), "uuid cannot be empty");
    this.uuid = uuid;
  }

  public String uuid() {
    return uuid;
  }

  @Override
  public Map<String, String> props() {
    return Map.of(
        UC_CREDENTIAL_CACHE_SCOPE_KEY,
        UC_CREDENTIAL_CACHE_SCOPE_QUERY,
        UC_CREDENTIALS_UID_KEY,
        uuid);
  }

  public static boolean isQueryScoped(Configuration conf) {
    return CredentialCacheScope.QUERY
        .value()
        .equals(conf.get(UC_CREDENTIAL_CACHE_SCOPE_KEY, UC_CREDENTIAL_CACHE_SCOPE_DEFAULT_VALUE));
  }

  /**
   * Resolves the credential cache key for {@code conf}.
   *
   * <p>Returns a query-scoped composite key when cache scope is {@code query}; otherwise returns
   * the resource {@link CredId}.
   */
  public static CredId resolveCacheKey(Configuration conf, Supplier<CredId> defaultCredId) {
    CredId credId = CredId.create(conf, defaultCredId);
    if (isQueryScoped(conf)) {
      return new QueryScopedCacheKey(credId, new QueryCredId(requireUuid(conf)));
    }
    return credId;
  }

  static String requireUuid(Configuration conf) {
    String uuid = conf.get(UC_CREDENTIALS_UID_KEY);
    Preconditions.checkState(
        uuid != null && !uuid.isEmpty(),
        "Query-scoped credential cache requires '%s' in hadoop configuration",
        UC_CREDENTIALS_UID_KEY);
    return uuid;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryCredId)) {
      return false;
    }
    QueryCredId that = (QueryCredId) o;
    return Objects.equals(uuid, that.uuid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uuid);
  }

  @Override
  public String toString() {
    return "QueryCredId{uuid=" + uuid + "}";
  }

  /** Cache key combining a resource {@link CredId} with a per-query {@link QueryCredId}. */
  private static final class QueryScopedCacheKey implements CredId {
    private final CredId credId;
    private final QueryCredId queryCredId;

    private QueryScopedCacheKey(CredId credId, QueryCredId queryCredId) {
      this.credId = credId;
      this.queryCredId = queryCredId;
    }

    @Override
    public Map<String, String> props() {
      Map<String, String> merged = new LinkedHashMap<>(credId.props());
      merged.putAll(queryCredId.props());
      return Map.copyOf(merged);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof QueryScopedCacheKey)) {
        return false;
      }
      QueryScopedCacheKey that = (QueryScopedCacheKey) o;
      return Objects.equals(credId, that.credId) && Objects.equals(queryCredId, that.queryCredId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(credId, queryCredId);
    }

    @Override
    public String toString() {
      return "QueryScopedCacheKey{credId=" + credId + ", queryCredId=" + queryCredId + "}";
    }
  }
}
