package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import java.util.ArrayList;
import java.util.Comparator;
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

  static void setRequiredCredential(String prefix, AwsCredential credential) {
    EXPECTED_CREDENTIALS.put(new Path(prefix).toString(), credential);
  }

  static void clearExpectedCredentials() {
    EXPECTED_CREDENTIALS.clear();
  }

  @Override
  protected void checkCredentials(Path f) {
    if (!credentialCheckEnabled) {
      return;
    }
    String uri = new Path(f.toUri()).toString();
    String expectedPrefix =
        EXPECTED_CREDENTIALS.keySet().stream()
            .filter(prefix -> covers(uri, prefix))
            .max(Comparator.comparingInt(String::length))
            .orElseThrow(() -> new AssertionError("no expected credential registered for " + f));
    String expectedAccessKey = EXPECTED_CREDENTIALS.get(expectedPrefix).accessKeyId();
    Configuration conf = getConf();

    GenericCredentialFetcher fetcher = () -> new ArrayList<>(EXPECTED_CREDENTIALS.values());
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

  private static boolean covers(String uri, String prefix) {
    return (uri + "/").startsWith(prefix + "/");
  }
}
