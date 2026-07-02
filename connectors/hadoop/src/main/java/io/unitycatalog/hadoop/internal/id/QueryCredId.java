package io.unitycatalog.hadoop.internal.id;

import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIAL_CACHE_SCOPE_DEFAULT_VALUE;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIAL_CACHE_SCOPE_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIAL_CACHE_SCOPE_QUERY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_QUERY_CRED_ID_KEY;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;

/**
 * Credential cache key that isolates vended credentials to a single query.
 *
 * <p>When {@link #isQueryScoped(Configuration)} is true, the global credential cache keys entries
 * by {@code (CredId, queryId)} instead of {@link CredId} alone, so separate queries do not reuse
 * each other's credentials even when they target the same table or path.
 */
public final class QueryCredId {
  private final CredId scope;
  private final String queryId;

  public QueryCredId(CredId scope, String queryId) {
    Preconditions.checkNotNull(scope, "scope is required");
    Preconditions.checkNotNull(queryId, "queryId is required");
    Preconditions.checkArgument(!queryId.isEmpty(), "queryId cannot be empty");
    this.scope = scope;
    this.queryId = queryId;
  }

  public CredId scope() {
    return scope;
  }

  public String queryId() {
    return queryId;
  }

  /** Returns the Hadoop properties needed to reconstruct this query-scoped cache key. */
  public Map<String, String> props() {
    return Map.of(
        UC_CREDENTIAL_CACHE_SCOPE_KEY,
        UC_CREDENTIAL_CACHE_SCOPE_QUERY,
        UC_QUERY_CRED_ID_KEY,
        queryId);
  }

  public static boolean isQueryScoped(Configuration conf) {
    return UC_CREDENTIAL_CACHE_SCOPE_QUERY.equals(
        conf.get(UC_CREDENTIAL_CACHE_SCOPE_KEY, UC_CREDENTIAL_CACHE_SCOPE_DEFAULT_VALUE));
  }

  /**
   * Resolves the credential cache key for {@code conf}.
   *
   * <p>Returns a {@link QueryCredId} when the cache scope is {@code query}; otherwise returns the
   * credential scope {@link CredId}.
   */
  public static Object resolveCacheKey(Configuration conf, Supplier<CredId> defaultCredId) {
    CredId scope = CredId.create(conf, defaultCredId);
    if (isQueryScoped(conf)) {
      return new QueryCredId(scope, requireQueryId(conf));
    }
    return scope;
  }

  private static String requireQueryId(Configuration conf) {
    String queryId = conf.get(UC_QUERY_CRED_ID_KEY);
    Preconditions.checkState(
        queryId != null && !queryId.isEmpty(),
        "Query-scoped credential cache requires '%s' in hadoop configuration",
        UC_QUERY_CRED_ID_KEY);
    return queryId;
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
    return Objects.equals(scope, that.scope) && Objects.equals(queryId, that.queryId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scope, queryId);
  }

  @Override
  public String toString() {
    return "QueryCredId{scope=" + scope + ", queryId=" + queryId + "}";
  }
}
