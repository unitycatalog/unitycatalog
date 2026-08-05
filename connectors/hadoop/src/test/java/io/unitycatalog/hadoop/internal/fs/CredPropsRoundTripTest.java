package io.unitycatalog.hadoop.internal.fs;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.CredPropsUtil;
import io.unitycatalog.hadoop.internal.CredentialUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/** Round-trip tests for credential properties produced and consumed during filesystem setup. */
class CredPropsRoundTripTest {

  private static final String LOCATION = "s3://bucket/table";

  @BeforeEach
  @AfterEach
  void reset() {
    CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
    CredScopedFileSystem.clearCacheForTesting();
  }

  @Test
  void singleCredentialRoundTripsThroughCredScopedFileSystemAndProvider() throws Exception {
    AwsCredential credential =
        new AwsCredential("access-key", "secret-key", "session-token", null, LOCATION);
    CredPropsUtil.genericCredFetcherFactory = (apiClient, credId) -> () -> List.of(credential);

    Configuration initialConf = new Configuration(false);
    initialConf.setBoolean(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, false);
    initialConf.set("fs.s3.impl", RawLocalFileSystem.class.getName());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            /* renewCredEnabled= */ true,
            /* credScopedFsEnabled= */ true,
            initialConf,
            "s3",
            /* apiClient= */ null,
            "http://uc",
            TokenProvider.create(Map.of("type", "static", "token", "token")),
            "table-id",
            UCCredentialHadoopConfs.TableOperation.READ,
            Map.of());

    Configuration confWithCreds = new Configuration(false);
    props.forEach(confWithCreds::set);
    CredScopedFileSystem fs = new CredScopedFileSystem();
    fs.initialize(new URI(LOCATION + "/part-0.parquet"), confWithCreds);

    Configuration delegateConf = fs.getDelegate().getConf();
    AwsVendedTokenProvider provider = new AwsVendedTokenProvider(delegateConf);
    AwsSessionCredentials resolved = (AwsSessionCredentials) provider.resolveCredentials();

    assertThat(provider.accessCredentials().prefix()).isEqualTo(LOCATION);
    assertThat(resolved.accessKeyId()).isEqualTo("access-key");
    assertThat(resolved.secretAccessKey()).isEqualTo("secret-key");
    assertThat(resolved.sessionToken()).isEqualTo("session-token");
  }

  @Test
  void multipleCredentialsRoundTripThroughCredScopedFileSystemAndProvider() throws Exception {
    AwsCredential credential =
        new AwsCredential("access-key", "secret-key", "session-token", null, LOCATION);
    CredPropsUtil.genericCredFetcherFactory = (apiClient, credId) -> () -> List.of(credential);

    Configuration initialConf = new Configuration(false);
    initialConf.setBoolean(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, false);
    initialConf.set("fs.s3.impl", RawLocalFileSystem.class.getName());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            /* renewCredEnabled= */ true,
            /* credScopedFsEnabled= */ true,
            initialConf,
            "s3",
            /* apiClient= */ null,
            "http://uc",
            TokenProvider.create(Map.of("type", "static", "token", "token")),
            "table-id",
            UCCredentialHadoopConfs.TableOperation.READ,
            Map.of());

    Configuration confWithCreds = new Configuration(false);
    props.forEach(confWithCreds::set);

    confWithCreds.setInt(UCHadoopConfConstants.UC_MULTI_CRED_COUNT_KEY, 2);

    // TODO: Replace these manually encoded properties with CredPropsUtil when it supports
    // producing properties for multiple vended credentials.
    setScopedAwsCredential(confWithCreds, 0, "s3://bucket", "parent-ak", "parent-sk", "parent-st");
    setScopedAwsCredential(confWithCreds, 1, LOCATION, "child-ak", "child-sk", "child-st");

    CredScopedFileSystem fs = new CredScopedFileSystem();
    fs.initialize(new URI(LOCATION + "/part-0.parquet"), confWithCreds);

    AwsVendedTokenProvider provider = new AwsVendedTokenProvider(fs.getDelegate().getConf());
    AwsSessionCredentials resolved = (AwsSessionCredentials) provider.resolveCredentials();

    assertThat(provider.accessCredentials().prefix()).isEqualTo(LOCATION);
    assertThat(resolved.accessKeyId()).isEqualTo("child-ak");
    assertThat(resolved.secretAccessKey()).isEqualTo("child-sk");
    assertThat(resolved.sessionToken()).isEqualTo("child-st");
  }

  private static void setScopedAwsCredential(
      Configuration conf,
      int index,
      String location,
      String accessKey,
      String secretKey,
      String sessionToken) {
    String namespace = CredentialUtil.hadoopConfNamespaceForIndex(index);
    conf.set(namespace + UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, location);
    conf.set(namespace + UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, accessKey);
    conf.set(namespace + UCHadoopConfConstants.S3A_INIT_SECRET_KEY, secretKey);
    conf.set(namespace + UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, sessionToken);
  }
}
