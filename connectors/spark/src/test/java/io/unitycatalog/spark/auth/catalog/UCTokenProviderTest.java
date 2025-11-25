package io.unitycatalog.spark.auth.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.spark.UCHadoopConf;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class UCTokenProviderTest {

  private static final String OAUTH_URI = "https://oauth.example.com/token";
  private static final String CLIENT_ID = "test-client-id";
  private static final String CLIENT_SECRET = "test-client-secret";

  @Test
  public void testCreateUCTokenProviderViaOptions() {
    // Test with valid token - should create FixedUCTokenProvider
    Map<String, String> tokenOptions = Map.of(OptionsUtil.TOKEN, "test-token");
    UCTokenProvider tokenProvider = UCTokenProvider.create(tokenOptions);
    assertThat(tokenProvider).isInstanceOf(FixedUCTokenProvider.class);
    assertThat(tokenProvider.accessToken()).isEqualTo("test-token");

    // Test with complete OAuth config - should create OAuthUCTokenProvider
    UCTokenProvider oauthProvider =
        UCTokenProvider.create(
            Map.of(
                OptionsUtil.OAUTH_URI, OAUTH_URI,
                OptionsUtil.OAUTH_CLIENT_ID, CLIENT_ID,
                OptionsUtil.OAUTH_CLIENT_SECRET, CLIENT_SECRET));
    assertThat(oauthProvider).isInstanceOf(OAuthUCTokenProvider.class);
    assertThat(oauthProvider.properties())
        .containsEntry(UCHadoopConf.UC_OAUTH_URI, OAUTH_URI)
        .containsEntry(UCHadoopConf.UC_OAUTH_CLIENT_ID, CLIENT_ID)
        .containsEntry(UCHadoopConf.UC_OAUTH_CLIENT_SECRET, CLIENT_SECRET);

    // Test with incomplete OAuth config - should throw
    Map<String, String> incompleteOAuthOptions = Map.of(OptionsUtil.OAUTH_URI, OAUTH_URI);
    assertThatThrownBy(() -> UCTokenProvider.create(incompleteOAuthOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Incomplete OAuth configuration detected");

    // Test with no valid config - should throw
    assertThatThrownBy(() -> UCTokenProvider.create(Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot determine UC authentication configuration from options");

    // Test token takes precedence over OAuth when both are present
    Map<String, String> bothOptions = new HashMap<>();
    bothOptions.put(OptionsUtil.TOKEN, "fixed-token");
    bothOptions.put(OptionsUtil.OAUTH_URI, OAUTH_URI);
    bothOptions.put(OptionsUtil.OAUTH_CLIENT_ID, CLIENT_ID);
    bothOptions.put(OptionsUtil.OAUTH_CLIENT_SECRET, CLIENT_SECRET);
    UCTokenProvider precedenceProvider = UCTokenProvider.create(bothOptions);
    assertThat(precedenceProvider).isInstanceOf(FixedUCTokenProvider.class);
    assertThat(precedenceProvider.accessToken()).isEqualTo("fixed-token");
  }

  @Test
  public void testCreateUCTokenProviderViaConf() {
    // Test with Configuration containing valid token
    Configuration tokenConf = new Configuration();
    tokenConf.set(UCHadoopConf.UC_TOKEN_KEY, "conf-token");
    UCTokenProvider confTokenProvider = UCTokenProvider.create(tokenConf);
    assertThat(confTokenProvider).isInstanceOf(FixedUCTokenProvider.class);
    assertThat(confTokenProvider.accessToken()).isEqualTo("conf-token");

    // Test with Configuration containing complete OAuth config
    Configuration oauthConf = new Configuration();
    oauthConf.set(UCHadoopConf.UC_OAUTH_URI, OAUTH_URI);
    oauthConf.set(UCHadoopConf.UC_OAUTH_CLIENT_ID, CLIENT_ID);
    oauthConf.set(UCHadoopConf.UC_OAUTH_CLIENT_SECRET, CLIENT_SECRET);
    UCTokenProvider confOAuthProvider = UCTokenProvider.create(oauthConf);
    assertThat(confOAuthProvider).isInstanceOf(OAuthUCTokenProvider.class);
    assertThat(confOAuthProvider.properties())
        .containsEntry(UCHadoopConf.UC_OAUTH_URI, OAUTH_URI)
        .containsEntry(UCHadoopConf.UC_OAUTH_CLIENT_ID, CLIENT_ID)
        .containsEntry(UCHadoopConf.UC_OAUTH_CLIENT_SECRET, CLIENT_SECRET);

    // Test with Configuration containing incomplete OAuth config - should throw
    Configuration incompleteOAuthConf = new Configuration();
    incompleteOAuthConf.set(UCHadoopConf.UC_OAUTH_URI, OAUTH_URI);
    assertThatThrownBy(() -> UCTokenProvider.create(incompleteOAuthConf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Incomplete OAuth configuration detected");

    // Test with empty Configuration - should throw
    Configuration emptyConf = new Configuration();
    assertThatThrownBy(() -> UCTokenProvider.create(emptyConf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot determine UC authentication configuration from options");

    // Test token takes precedence over OAuth in Configuration
    Configuration bothConf = new Configuration();
    bothConf.set(UCHadoopConf.UC_TOKEN_KEY, "conf-fixed-token");
    bothConf.set(UCHadoopConf.UC_OAUTH_URI, OAUTH_URI);
    bothConf.set(UCHadoopConf.UC_OAUTH_CLIENT_ID, CLIENT_ID);
    bothConf.set(UCHadoopConf.UC_OAUTH_CLIENT_SECRET, CLIENT_SECRET);
    UCTokenProvider confPrecedenceProvider = UCTokenProvider.create(bothConf);
    assertThat(confPrecedenceProvider).isInstanceOf(FixedUCTokenProvider.class);
    assertThat(confPrecedenceProvider.accessToken()).isEqualTo("conf-fixed-token");
  }

  @Test
  public void testFixedUCTokenProvider() {
    FixedUCTokenProvider provider = new FixedUCTokenProvider("my-token");
    assertThat(provider.accessToken()).isEqualTo("my-token");

    FixedUCTokenProvider consistentProvider = new FixedUCTokenProvider("consistent");
    assertThat(consistentProvider.accessToken()).isEqualTo("consistent");
    assertThat(consistentProvider.accessToken()).isEqualTo("consistent");
    assertThat(consistentProvider.accessToken()).isEqualTo("consistent");

    FixedUCTokenProvider providerWithProperties = new FixedUCTokenProvider("test-token");
    assertThat(providerWithProperties.properties())
        .hasSize(1)
        .containsEntry(UCHadoopConf.UC_TOKEN_KEY, "test-token");

    FixedUCTokenProvider factoryProvider = FixedUCTokenProvider.create("factory-token");
    assertThat(factoryProvider).isNotNull();
    assertThat(factoryProvider.accessToken()).isEqualTo("factory-token");
  }
}
