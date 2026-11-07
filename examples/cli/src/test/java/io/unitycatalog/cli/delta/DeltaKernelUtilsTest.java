package io.unitycatalog.cli.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DeltaKernelUtilsTest {

  private static final String AWS_ENDPOINT_PROP = "aws.endpoint";
  private String savedEndpoint;

  private TemporaryCredentials creds() {
    return new TemporaryCredentials()
        .awsTempCredentials(
            new AwsCredentials()
                .accessKeyId("AKIDEMO")
                .secretAccessKey("SECRETDEMO")
                .sessionToken("TOKENDEMO"));
  }

  @BeforeEach
  void saveEndpoint() {
    savedEndpoint = System.getProperty(AWS_ENDPOINT_PROP);
    System.clearProperty(AWS_ENDPOINT_PROP);
  }

  @AfterEach
  void restoreEndpoint() {
    if (savedEndpoint == null) {
      System.clearProperty(AWS_ENDPOINT_PROP);
    } else {
      System.setProperty(AWS_ENDPOINT_PROP, savedEndpoint);
    }
  }

  @Test
  void s3Config_setsEndpointFromAwsEndpointSystemProperty() {
    System.setProperty(AWS_ENDPOINT_PROP, "http://localhost:8333");

    Configuration conf =
        DeltaKernelUtils.getHDFSConfiguration(URI.create("s3://lakehouse/warehouse"), creds());

    assertThat(conf.get("fs.s3a.endpoint")).isEqualTo("http://localhost:8333");
    assertThat(conf.get("fs.s3a.path.style.access")).isEqualTo("true");
  }

  @Test
  void s3Config_doesNotSetEndpointWhenUnset() {
    Configuration conf =
        DeltaKernelUtils.getHDFSConfiguration(URI.create("s3://lakehouse/warehouse"), creds());

    assertThat(conf.get("fs.s3a.endpoint")).isNull();
  }
}
