package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Coverage for the S3-compatible endpoint vended alongside S3 credentials, which lets a client
 * reach a MinIO-style store instead of real AWS S3.
 */
class S3EndpointCredPropsTest {

  private static final String S3A_ENDPOINT = "fs.s3a.endpoint";
  private static final String VENDED_ENDPOINT = "http://localhost:9000";
  private static final String CLIENT_ENDPOINT = "http://minio:9000";

  @AfterEach
  void resetFetcher() {
    CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
    CredPropsUtil.initialCredCache.clear();
  }

  @ParameterizedTest(name = "renew={0}")
  @ValueSource(booleans = {true, false})
  void appliesVendedEndpointWhenClientConfiguredNone(boolean renew) throws Exception {
    Map<String, String> props =
        credProps(awsCred(VENDED_ENDPOINT), new Configuration(false), renew);

    assertThat(props)
        .containsEntry(S3A_ENDPOINT, VENDED_ENDPOINT)
        .containsEntry(UCHadoopConfConstants.S3A_INIT_ENDPOINT_URL, VENDED_ENDPOINT);
  }

  /** A client may reach the same store by a different address than the server does. */
  @ParameterizedTest(name = "renew={0}")
  @ValueSource(booleans = {true, false})
  void defersToClientConfiguredEndpoint(boolean renew) throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(S3A_ENDPOINT, CLIENT_ENDPOINT);

    Map<String, String> props = credProps(awsCred(VENDED_ENDPOINT), conf, renew);

    assertThat(props)
        .containsEntry(S3A_ENDPOINT, CLIENT_ENDPOINT)
        .containsEntry(UCHadoopConfConstants.S3A_INIT_ENDPOINT_URL, CLIENT_ENDPOINT);
  }

  @ParameterizedTest(name = "renew={0}")
  @ValueSource(booleans = {true, false})
  void omitsEndpointKeysWhenNoneVended(boolean renew) throws Exception {
    Map<String, String> props = credProps(awsCred(null), new Configuration(false), renew);

    assertThat(props)
        .doesNotContainKey(S3A_ENDPOINT)
        .doesNotContainKey(UCHadoopConfConstants.S3A_INIT_ENDPOINT_URL);
  }

  private static AwsCredential awsCred(String endpointUrl) {
    return new AwsCredential("ak", "sk", "st", Long.MAX_VALUE, null, endpointUrl);
  }

  private static Map<String, String> credProps(
      GenericCredential cred, Configuration conf, boolean renewCredEnabled) throws Exception {
    CredPropsUtil.genericCredFetcherFactory = (apiClient, credId) -> fetcher(cred);
    return CredPropsUtil.createTableCredProps(
        renewCredEnabled,
        false,
        conf,
        "s3",
        null,
        "http://uc",
        TokenProvider.create(Map.of("type", "static", "token", "tok")),
        "tid",
        UCCredentialHadoopConfs.TableOperation.READ_WRITE,
        Map.of());
  }

  private static GenericCredentialFetcher fetcher(GenericCredential cred) {
    GenericCredentialFetcher api = mock(GenericCredentialFetcher.class);
    try {
      when(api.createCredentials()).thenReturn(List.of(cred));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return api;
  }
}
