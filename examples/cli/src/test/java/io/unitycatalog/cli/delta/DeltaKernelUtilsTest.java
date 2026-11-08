package io.unitycatalog.cli.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class DeltaKernelUtilsTest {

  @Test
  public void testS3ConfigurationUsesVendedEndpointWithoutEmptySessionToken() {
    TemporaryCredentials credentials =
        new TemporaryCredentials()
            .awsTempCredentials(
                new AwsCredentials().accessKeyId("access-key").secretAccessKey("secret-key"))
            .endpointUrl("https://s3.example.test");

    Configuration configuration =
        DeltaKernelUtils.getHDFSConfiguration(URI.create("s3://bucket/table"), credentials);

    assertThat(configuration.get("fs.s3a.access.key")).isEqualTo("access-key");
    assertThat(configuration.get("fs.s3a.secret.key")).isEqualTo("secret-key");
    assertThat(configuration.get("fs.s3a.session.token")).isNull();
    assertThat(configuration.get("fs.s3a.endpoint")).isEqualTo("https://s3.example.test");
  }
}
