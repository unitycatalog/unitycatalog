package io.unitycatalog.hadoop.internal.id;

import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_UID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIAL_CACHE_SCOPE_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIAL_CACHE_SCOPE_QUERY;
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
    conf.set(
        UC_CREDENTIAL_CACHE_SCOPE_KEY,
        io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIAL_CACHE_SCOPE_CLUSTER);

    CredId cacheKey = QueryCredId.resolveCacheKey(conf, () -> new TableCredId("fallback", "READ"));

    assertThat(cacheKey).isEqualTo(new TableCredId("tableA", "READ"));
  }

  @Test
  void resolveCacheKeyUsesQueryScopedKeyForQueryScope() {
    Configuration conf = tableConfWithQueryScope("query-1");

    CredId cacheKey = QueryCredId.resolveCacheKey(conf, () -> new TableCredId("fallback", "READ"));

    assertThat(cacheKey.props())
        .containsEntry(UC_TABLE_ID_KEY, "tableA")
        .containsEntry(UC_CREDENTIALS_UID_KEY, "query-1");
  }

  @Test
  void resolveCacheKeyRequiresUuidForQueryScope() {
    Configuration conf = tableConf();
    conf.set(UC_CREDENTIAL_CACHE_SCOPE_KEY, UC_CREDENTIAL_CACHE_SCOPE_QUERY);

    assertThatThrownBy(
            () -> QueryCredId.resolveCacheKey(conf, () -> new TableCredId("fallback", "READ")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(UC_CREDENTIALS_UID_KEY);
  }

  @Test
  void propsIncludeScopeAndUuid() {
    QueryCredId queryCredId = new QueryCredId("query-1");

    assertThat(queryCredId.props())
        .containsEntry(UC_CREDENTIAL_CACHE_SCOPE_KEY, UC_CREDENTIAL_CACHE_SCOPE_QUERY)
        .containsEntry(UC_CREDENTIALS_UID_KEY, "query-1");
  }

  private static Configuration tableConf() {
    Configuration conf = new Configuration();
    conf.set(UC_CREDENTIALS_TYPE_KEY, UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UC_TABLE_ID_KEY, "tableA");
    conf.set(UC_TABLE_OPERATION_KEY, "READ");
    return conf;
  }

  private static Configuration tableConfWithQueryScope(String uuid) {
    Configuration conf = tableConf();
    conf.set(UC_CREDENTIAL_CACHE_SCOPE_KEY, UC_CREDENTIAL_CACHE_SCOPE_QUERY);
    conf.set(UC_CREDENTIALS_UID_KEY, uuid);
    return conf;
  }
}
