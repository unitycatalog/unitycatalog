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
 * Static S3 access keys are vended without a session token. Hadoop/S3A must get access/secret only
 * so it uses basic credentials; STS-vended credentials still carry a session token.
 */
class S3StaticAccessKeyCredPropsTest {

  private static final String S3A_ACCESS_KEY = "fs.s3a.access.key";
  private static final String S3A_SECRET_KEY = "fs.s3a.secret.key";
  private static final String S3A_SESSION_TOKEN = "fs.s3a.session.token";

  @AfterEach
  void resetFetcher() {
    CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
    CredPropsUtil.initialCredCache.clear();
  }

  @ParameterizedTest(name = "renew={0}")
  @ValueSource(booleans = {true, false})
  void omitsSessionTokenKeysWhenNoneVended(boolean renew) throws Exception {
    Map<String, String> props = credProps(awsCred(null), renew);

    assertThat(props)
        .doesNotContainKey(S3A_SESSION_TOKEN)
        .doesNotContainKey(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN);
    if (renew) {
      assertThat(props)
          .containsEntry(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak")
          .containsEntry(UCHadoopConfConstants.S3A_INIT_SECRET_KEY, "sk");
    } else {
      assertThat(props).containsEntry(S3A_ACCESS_KEY, "ak").containsEntry(S3A_SECRET_KEY, "sk");
    }
  }

  @ParameterizedTest(name = "renew={0}")
  @ValueSource(booleans = {true, false})
  void stillSetsSessionTokenWhenStsVended(boolean renew) throws Exception {
    Map<String, String> props = credProps(awsCred("st"), renew);

    if (renew) {
      assertThat(props).containsEntry(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, "st");
    } else {
      assertThat(props).containsEntry(S3A_SESSION_TOKEN, "st");
    }
  }

  private static AwsCredential awsCred(String sessionToken) {
    return new AwsCredential("ak", "sk", sessionToken, Long.MAX_VALUE, null);
  }

  private static Map<String, String> credProps(GenericCredential cred, boolean renewCredEnabled)
      throws Exception {
    CredPropsUtil.genericCredFetcherFactory = (apiClient, credId) -> fetcher(cred);
    return CredPropsUtil.createTableCredProps(
        renewCredEnabled,
        false,
        new Configuration(false),
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
