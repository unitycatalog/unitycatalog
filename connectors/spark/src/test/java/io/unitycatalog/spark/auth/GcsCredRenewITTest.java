package io.unitycatalog.spark.auth;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.google.auth.oauth2.AccessToken;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialsGenerator;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

public class GcsCredRenewITTest extends BaseCredRenewITTest {
  private static final String SCHEME = "gs";
  private static final String CREDENTIALS_GENERATOR_CLASS = GcsCredGenerator.class.getName();

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.put("gcs.bucketPath.0", "gs://" + BUCKET_NAME);
    serverProperties.put("gcs.credentialsGenerator.0", CREDENTIALS_GENERATOR_CLASS);
  }

  @Override
  protected String scheme() {
    return SCHEME;
  }

  @Override
  protected Map<String, String> catalogExtraProps() {
    return Map.of("fs.gs.impl", GcsCredFileSystem.class.getName());
  }

  public static class GcsCredGenerator extends TimeBasedCredGenerator<AccessToken>
      implements GcpCredentialsGenerator {
    @Override
    protected AccessToken newTimeBasedCred(long ts) {
      Instant expiration = Instant.ofEpochMilli(ts + DEFAULT_INTERVAL_MILLIS);
      return AccessToken.newBuilder()
          .setTokenValue(String.format("testing-renew://gs://%s#%d", BUCKET_NAME, ts))
          .setExpirationTime(Date.from(expiration))
          .build();
    }
  }

  public static class GcsCredFileSystem extends CredRenewFileSystem<AccessTokenProvider> {
    @Override
    protected String scheme() {
      return String.format("%s:", SCHEME);
    }

    @Override
    protected AccessTokenProvider createProvider() {
      String clazz = getConf().get("fs.gs.auth.access.token.provider");
      assertThat(clazz).isEqualTo(GcsVendedTokenProvider.class.getName());

      try {
        AccessTokenProvider provider = new GcsVendedTokenProvider();
        provider.setConf(getConf());
        return provider;
      } catch (Exception e) {
        throw new RuntimeException("Failed to initialize GCS token provider", e);
      }
    }

    @Override
    protected void assertCredentials(AccessTokenProvider provider, long ts) {
      AccessTokenProvider.AccessToken token = provider.getAccessToken();
      assertThat(token).isNotNull();
      assertThat(token.getToken())
          .isEqualTo(String.format("testing-renew://gs://%s#%d", BUCKET_NAME, ts));
    }
  }
}
