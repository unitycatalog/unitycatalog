package io.unitycatalog.server.service.credential.aws;

import java.net.URI;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class S3StorageConfig {
  private final String bucketPath;
  private final String region;
  private final String endpoint;
  private final String awsRoleArn;
  private final String accessKey;
  private final String secretKey;
  private final String sessionToken;

  public URI getEndpointURI() {
    try {
      return new URI(this.endpoint);
    } catch (Exception e) {
      return null;
    }
  }
}
