package io.unitycatalog.server.service.credential.aws;

import java.net.MalformedURLException;
import java.net.URL;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class S3StorageConfig {
  private final String bucketPath;
  private final String region;
  private final String awsRoleArn;
  private final String accessKey;
  private final String secretKey;
  private final String sessionToken;
  private final String credentialsGenerator;

  /**
   * Optional S3-compatible service endpoint URL (e.g., https://minio.example.com:9000). If null or
   * empty, AWS S3 is used (default behavior). Format: http[s]://hostname[:port]
   */
  @Builder.Default private final String serviceEndpoint = null;

  /**
   * Validates the serviceEndpoint URL format if provided.
   *
   * @throws IllegalArgumentException if serviceEndpoint is not a valid HTTP/HTTPS URL
   */
  public void validateServiceEndpoint() {
    if (serviceEndpoint != null && !serviceEndpoint.isEmpty()) {
      try {
        URL url = new URL(serviceEndpoint);
        String protocol = url.getProtocol();
        if (!protocol.equals("http") && !protocol.equals("https")) {
          throw new IllegalArgumentException(
              "Invalid service endpoint protocol: " + protocol + ". Must be http or https.");
        }
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(
            "Invalid service endpoint URL format: " + serviceEndpoint, e);
      }
    }
  }
}
