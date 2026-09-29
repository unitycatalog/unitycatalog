package io.unitycatalog.server.service.credential.cache;

import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class CachedCredentialTest {

  private static CredentialCacheContext context(String url) {
    NormalizedURL location = NormalizedURL.from(url);
    return new CredentialCacheContext(
        CredentialCacheContext.CURRENT_SCHEMA_VERSION,
        location,
        UriScheme.fromURI(location.toUri()),
        Set.of(SELECT),
        null,
        null,
        2_000L);
  }

  @ParameterizedTest(name = "scalar snapshot: mutateSource={0}")
  @ValueSource(booleans = {true, false})
  void scalarFieldsAreIsolatedFromMutation(boolean mutateSource) {
    TemporaryCredentials source =
        new TemporaryCredentials().url("s3://bucket/table").expirationTime(1_000L);
    CachedCredential cached = new CachedCredential(context(source.getUrl()), source);

    TemporaryCredentials mutable = mutateSource ? source : cached.credential();
    mutable.setUrl("s3://other/table");
    mutable.setExpirationTime(9_000L);

    TemporaryCredentials actual = cached.credential();
    assertEquals("s3://bucket/table", actual.getUrl());
    assertEquals(1_000L, actual.getExpirationTime());
    assertNull(actual.getAwsTempCredentials());
    assertNull(actual.getAzureUserDelegationSas());
    assertNull(actual.getGcpOauthToken());
  }

  @ParameterizedTest(name = "AWS snapshot: mutateSource={0}")
  @ValueSource(booleans = {true, false})
  void awsFieldsAreIsolatedFromMutation(boolean mutateSource) {
    TemporaryCredentials source =
        new TemporaryCredentials()
            .url("s3://bucket/table")
            .awsTempCredentials(
                new AwsCredentials()
                    .accessKeyId("access")
                    .secretAccessKey("secret")
                    .sessionToken("session"));
    CachedCredential cached = new CachedCredential(context(source.getUrl()), source);

    TemporaryCredentials mutable = mutateSource ? source : cached.credential();
    mutable.getAwsTempCredentials().setAccessKeyId("changed-access");
    mutable.getAwsTempCredentials().setSecretAccessKey("changed-secret");
    mutable.getAwsTempCredentials().setSessionToken("changed-session");

    AwsCredentials actual = cached.credential().getAwsTempCredentials();
    assertEquals("access", actual.getAccessKeyId());
    assertEquals("secret", actual.getSecretAccessKey());
    assertEquals("session", actual.getSessionToken());
  }

  @ParameterizedTest(name = "Azure snapshot: mutateSource={0}")
  @ValueSource(booleans = {true, false})
  void azureFieldsAreIsolatedFromMutation(boolean mutateSource) {
    TemporaryCredentials source =
        new TemporaryCredentials()
            .url("abfs://container@account.dfs.core.windows.net/table")
            .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("sas"));
    CachedCredential cached = new CachedCredential(context(source.getUrl()), source);

    TemporaryCredentials mutable = mutateSource ? source : cached.credential();
    mutable.getAzureUserDelegationSas().setSasToken("changed-sas");

    assertEquals("sas", cached.credential().getAzureUserDelegationSas().getSasToken());
  }

  @ParameterizedTest(name = "GCP snapshot: mutateSource={0}")
  @ValueSource(booleans = {true, false})
  void gcpFieldsAreIsolatedFromMutation(boolean mutateSource) {
    TemporaryCredentials source =
        new TemporaryCredentials()
            .url("gs://bucket/table")
            .gcpOauthToken(new GcpOauthToken().oauthToken("oauth"));
    CachedCredential cached = new CachedCredential(context(source.getUrl()), source);

    TemporaryCredentials mutable = mutateSource ? source : cached.credential();
    mutable.getGcpOauthToken().setOauthToken("changed-oauth");

    assertEquals("oauth", cached.credential().getGcpOauthToken().getOauthToken());
  }

  @Test
  void toStringOmitsCredentialSecrets() {
    TemporaryCredentials source =
        new TemporaryCredentials()
            .url("s3://bucket/table")
            .awsTempCredentials(
                new AwsCredentials()
                    .accessKeyId("test-access")
                    .secretAccessKey("test-secret")
                    .sessionToken("test-session"))
            .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("test-sas"))
            .gcpOauthToken(new GcpOauthToken().oauthToken("test-oauth"));
    CachedCredential cached = new CachedCredential(context(source.getUrl()), source);

    assertThat(cached.toString())
        .contains("s3://bucket/table")
        .doesNotContain("test-access", "test-secret", "test-session", "test-sas", "test-oauth");
  }

  @Test
  void absentFieldsRemainAbsent() {
    CachedCredential cached =
        new CachedCredential(context("s3://bucket/table"), new TemporaryCredentials());

    assertEquals(new TemporaryCredentials(), cached.credential());
  }

  @Test
  void nullArgumentsAreRejected() {
    assertThrows(
        NullPointerException.class, () -> new CachedCredential(null, new TemporaryCredentials()));
    assertThrows(
        NullPointerException.class, () -> new CachedCredential(context("s3://bucket/table"), null));
  }
}
