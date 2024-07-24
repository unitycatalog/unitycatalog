package io.unitycatalog.server.service.credential.azure;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ADLSStorageConfig {
  private String containerPath;
  private String tenantId;
  private String clientId;
  private String clientSecret;
}
