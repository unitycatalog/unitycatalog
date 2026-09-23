package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

import io.unitycatalog.hadoop.internal.CredentialUtil;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.mockito.MockedStatic;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * A fake S3 filesystem that validates the vended credential against the path <em>prefix</em> being
 * accessed. Tests register the expected credential for each URI prefix, which supports arbitrary
 * sibling and nested layouts.
 */
public class PerPathCredentialTestFileSystem extends CredentialTestFileSystem {

  private static final Map<String, AwsCredential> EXPECTED_CREDENTIALS = new ConcurrentHashMap<>();
  private static volatile List<GenericCredential> vendedCredentials = List.of();

  static void setRequiredCredential(String prefix, AwsCredential credential) {
    EXPECTED_CREDENTIALS.put(new Path(prefix).toString(), credential);
  }

  static void clearExpectedCredentials() {
    EXPECTED_CREDENTIALS.clear();
  }

  static void setVendedCredentials(List<GenericCredential> credentials) {
    vendedCredentials = List.copyOf(credentials);
  }

  static void clearVendedCredentials() {
    vendedCredentials = List.of();
  }

  @Override
  protected void checkCredentials(Path f) {
    if (!credentialCheckEnabled) {
      return;
    }
    String uri = new Path(f.toUri()).toString();
    List<String> prefixes = new ArrayList<>(EXPECTED_CREDENTIALS.keySet());
    int expectedPrefixIndex = CredentialUtil.longestCoveringIndex(uri, prefixes);
    assertThat(expectedPrefixIndex)
        .as("expected a credential to be registered for %s", f)
        .isNotNegative();
    String expectedPrefix = prefixes.get(expectedPrefixIndex);
    String expectedAccessKey = EXPECTED_CREDENTIALS.get(expectedPrefix).accessKeyId();
    Configuration conf = getConf();

    GenericCredentialFetcher fetcher = () -> vendedCredentials;
    try (MockedStatic<GenericCredentialFetcher> mockedFetcher =
        mockStatic(GenericCredentialFetcher.class)) {
      mockedFetcher.when(() -> GenericCredentialFetcher.create(conf)).thenReturn(fetcher);

      AwsSessionCredentials credentials =
          (AwsSessionCredentials) new AwsVendedTokenProvider(conf).resolveCredentials();
      assertThat(credentials.accessKeyId())
          .as("expected access key %s for %s", expectedAccessKey, f)
          .isEqualTo(expectedAccessKey);
    }
  }

  @Override
  protected String scheme() {
    return "s3:";
  }
}
