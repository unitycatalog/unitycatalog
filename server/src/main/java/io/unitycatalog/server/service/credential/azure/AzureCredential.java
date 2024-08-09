package io.unitycatalog.server.service.credential.azure;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class AzureCredential {
  private String sasToken;
  private long expirationTimeInEpochMillis;
}
