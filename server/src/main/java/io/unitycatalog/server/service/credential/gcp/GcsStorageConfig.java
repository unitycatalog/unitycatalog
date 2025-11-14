package io.unitycatalog.server.service.credential.gcp;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class GcsStorageConfig {
  private final String bucketPath;
  private final String jsonKeyFilePath;
  private final String credentialsGenerator;
}
