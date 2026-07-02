package io.unitycatalog.hadoop.internal.id;

import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIAL_CACHE_SCOPE_CLUSTER;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIAL_CACHE_SCOPE_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIAL_CACHE_SCOPE_QUERY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_QUERY_CRED_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_TABLE_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_TABLE_OPERATION_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class QueryCredIdTest {

  @Test
  void resolveCacheKeyUsesCredIdForClusterScope() {
    Configuration conf = tableConf();
    conf.set(UC_CREDENTIAL_CACHE_SCOPE_KEY, UC_CREDENTIAL_CACHE_SCOPE_CLUSTER);

    Object cacheKey = QueryCredId.resolveCacheKey(conf, () -> new TableCredId("fallback", "READ"));

    assertThat(cacheKey).isInstanceOf(TableCredId.class);
    assertThat(cacheKey).isEqualTo(new TableCredId("tableA", "READ"));
  }

  @Test
  void resolveCacheKeyUsesQueryCredIdForQueryScope() {
    Configuration conf = tableConf();
    conf.set(UC_CREDENTIAL_CACHE_SCOPE_KEY, UC_CREDENTIAL_CACHE_SCOPE_QUERY);
    conf.set(UC_QUERY_CRED_ID_KEY, "query-1");

    Object cacheKey = QueryCredId.resolveCacheKey(conf, () -> new TableCredId("fallback", "READ"));

    assertThat(cacheKey).isEqualTo(new QueryCredId(new TableCredId("tableA", "READ"), "query-1"));
  }

  @Test
  void resolveCacheKeyRequiresQueryIdForQueryScope() {
    Configuration conf = tableConf();
    conf.set(UC_CREDENTIAL_CACHE_SCOPE_KEY, UC_CREDENTIAL_CACHE_SCOPE_QUERY);

    assertThatThrownBy(
            () -> QueryCredId.resolveCacheKey(conf, () -> new TableCredId("fallback", "READ")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(UC_QUERY_CRED_ID_KEY);
  }

  @Test
  void propsIncludeScopeAndQueryId() {
    QueryCredId queryCredId = new QueryCredId(new TableCredId("tableA", "READ"), "query-1");

    assertThat(queryCredId.props())
        .containsEntry(UC_CREDENTIAL_CACHE_SCOPE_KEY, UC_CREDENTIAL_CACHE_SCOPE_QUERY)
        .containsEntry(UC_QUERY_CRED_ID_KEY, "query-1");
  }

  private static Configuration tableConf() {
    Configuration conf = new Configuration();
    conf.set(UC_CREDENTIALS_TYPE_KEY, UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UC_TABLE_ID_KEY, "tableA");
    conf.set(UC_TABLE_OPERATION_KEY, "READ");
    return conf;
  }
}
