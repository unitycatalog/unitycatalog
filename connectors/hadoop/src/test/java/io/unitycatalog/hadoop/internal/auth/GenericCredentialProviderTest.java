package io.unitycatalog.hadoop.internal.auth;

import static io.unitycatalog.hadoop.internal.id.CredIdTest.EMPTY_CRED_CONTEXT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.OngoingStubbing;

/**
 * Verifies how {@link GenericCredentialProvider} turns the (possibly multi-element) list its
 * fetcher returns into the single credential it serves: it selects by the location the provider is
 * scoped to, caches the selected credential, and renews it.
 */
class GenericCredentialProviderTest {
  private String clockName;
  private Clock clock;

  @BeforeEach
  void before() {
    clockName = UUID.randomUUID().toString();
    clock = Clock.getManualClock(clockName);
    GenericCredentialProvider.globalCache.clear();
  }

  @AfterEach
  void after() {
    Clock.removeManualClock(clockName);
    clock = null;
    clockName = null;
    GenericCredentialProvider.globalCache.clear();
  }

  @Test
  void selectsCredentialCoveringLocationByLongestPrefix() {
    GenericCredential bucket = awsAt("s3://bucket", Long.MAX_VALUE);
    GenericCredential table = awsAt("s3://bucket/t", Long.MAX_VALUE);
    GenericCredential child = awsAt("s3://bucket/t/child", Long.MAX_VALUE);

    TestProvider provider =
        provider().location("s3://bucket/t/child/file").vending(bucket, table, child);

    assertThat(provider.accessCredentials()).isSameAs(child);
  }

  @Test
  void selectedCredentialIsSharedAcrossProvidersWithoutRefetch() throws Exception {
    GenericCredential firstSelected = awsAt("s3://bucket/t", Long.MAX_VALUE);
    GenericCredential secondSelected = awsAt("s3://bucket/t", Long.MAX_VALUE);

    // Two providers scoped to the same credential + location share one global cache entry.
    TestProvider first =
        provider()
            .location("s3://bucket/t/file")
            .vending(awsAt("s3://bucket", Long.MAX_VALUE), firstSelected);
    TestProvider second =
        provider()
            .location("s3://bucket/t/file")
            .vending(awsAt("s3://bucket", Long.MAX_VALUE), secondSelected);

    // First access selects the covering credential and stores that single credential in the cache.
    assertThat(first.accessCredentials()).isSameAs(firstSelected);
    assertThat(GenericCredentialProvider.globalCache.values()).containsExactly(firstSelected);

    // The second provider reuses the cached credential without consulting its own fetcher, even
    // though that fetcher would have selected the distinct secondSelected.
    assertThat(second.accessCredentials()).isSameAs(firstSelected);
    verify(second.fetcher, never()).createCredentials();
  }

  @Test
  void renewalReselectsFromTheRefetchedList() throws Exception {
    long soon = clock.now().toEpochMilli() + 2000L;
    GenericCredential table1 = awsAt("s3://bucket/t", soon);
    GenericCredential table2 = awsAt("s3://bucket/t", Long.MAX_VALUE);

    TestProvider provider =
        provider()
            .location("s3://bucket/t/file")
            .leadTime(1000L)
            .vendingInSequence(
                Arrays.asList(awsAt("s3://bucket", Long.MAX_VALUE), table1),
                Arrays.asList(awsAt("s3://bucket", Long.MAX_VALUE), table2));

    // First access selects table1 (longest cover) and caches it.
    assertThat(provider.accessCredentials()).isSameAs(table1);

    // Advance the clock into table1's renewal window; the next access refetches and re-selects.
    clock.sleep(Duration.ofMillis(1500));
    assertThat(provider.accessCredentials()).isSameAs(table2);
    verify(provider.fetcher, times(2)).createCredentials();
  }

  @Test
  void singleCredentialIsUsedWithoutMatchingLocation() {
    GenericCredential only = awsAt("s3://other-bucket", Long.MAX_VALUE);

    TestProvider provider = provider().location("s3://bucket/t/file").vending(only);

    assertThat(provider.accessCredentials()).isSameAs(only);
  }

  @Test
  void throwsWhenNoCredentialIsVended() {
    TestProvider provider = provider().location("s3://bucket/t").vending();

    assertThatThrownBy(provider::accessCredentials)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No vended credential was returned.");
  }

  @Test
  void throwsWhenMultipleCredentialsButNoLocation() {
    // Location must be set to select the appropriate credential.
    TestProvider provider =
        provider()
            .vending(
                awsAt("s3://bucket/a", Long.MAX_VALUE), awsAt("s3://bucket/b", Long.MAX_VALUE));

    assertThatThrownBy(provider::accessCredentials)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Multiple credentials were vended but no location");
  }

  @Test
  void throwsWhenNoCredentialCoversLocation() {
    TestProvider provider =
        provider()
            .location("s3://bucket/t")
            .vending(
                awsAt("s3://other", Long.MAX_VALUE), awsAt("s3://bucket/sibling", Long.MAX_VALUE));

    assertThatThrownBy(provider::accessCredentials)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No vended credential covers location");
  }

  @Test
  void selectsCredentialWhenCacheDisabledAndBypassesSharedCache() {
    GenericCredential table = awsAt("s3://bucket/t", Long.MAX_VALUE);

    TestProvider provider =
        provider()
            .location("s3://bucket/t/file")
            .cacheDisabled()
            .vending(awsAt("s3://bucket", Long.MAX_VALUE), table);

    assertThat(provider.accessCredentials()).isSameAs(table);
    // Cache disabled: the selected credential is served but never stored in the shared cache.
    assertThat(GenericCredentialProvider.globalCache.values()).isEmpty();
  }

  private Builder provider() {
    return new Builder();
  }

  private static GenericCredential awsAt(String location, long expirationMillis) {
    return new AwsCredential("ak", "sk", "st", expirationMillis, location);
  }

  /** The minimal conf that lets {@code initialize} build a credential scope. */
  private Configuration scopeConf() {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY, EMPTY_CRED_CONTEXT_ID);
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_TABLE_ID_KEY, "tid");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, TableOperation.READ.getValue());
    conf.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
    return conf;
  }

  /** Builds a {@link TestProvider} from a table-scoped conf, one setter per configurable knob. */
  private class Builder {
    private final Configuration conf = scopeConf();

    /** The location the provider selects on; unset means no location. */
    Builder location(String selectionLocation) {
      conf.set(UCHadoopConfConstants.UC_CREDENTIAL_LOCATION_KEY, selectionLocation);
      return this;
    }

    Builder leadTime(long leadTimeMillis) {
      conf.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, leadTimeMillis);
      return this;
    }

    Builder cacheDisabled() {
      conf.setBoolean(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, false);
      return this;
    }

    /** Every fetch returns the same vended list. */
    TestProvider vending(GenericCredential... creds) {
      return new TestProvider(conf, fetcherReturning(Arrays.asList(creds)));
    }

    /** Successive fetches return {@code first} then {@code second} (for renewal). */
    TestProvider vendingInSequence(List<GenericCredential> first, List<GenericCredential> second) {
      return new TestProvider(conf, fetcherReturning(first, second));
    }
  }

  /** A mock fetcher that returns each of {@code responses} on successive calls. */
  @SafeVarargs
  private static GenericCredentialFetcher fetcherReturning(List<GenericCredential>... responses) {
    GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
    try {
      OngoingStubbing<List<GenericCredential>> stub = when(fetcher.createCredentials());
      for (List<GenericCredential> response : responses) {
        stub = stub.thenReturn(response);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return fetcher;
  }

  /** A concrete provider whose fetcher is the given mock, so selection/caching/renewal run. */
  private static class TestProvider extends GenericCredentialProvider {
    private final GenericCredentialFetcher fetcher;

    TestProvider(Configuration conf, GenericCredentialFetcher fetcher) {
      this.fetcher = fetcher;
      initialize(conf);
    }

    @Override
    public GenericCredential initGenericCredential(Configuration conf) {
      return null;
    }

    @Override
    GenericCredentialFetcher genericCredentialFetcher() {
      return fetcher;
    }
  }
}
