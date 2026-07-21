package io.unitycatalog.server.service.iceberg;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link TableConfigService} vends storage credentials with the privileges the caller
 * asks for: read-only by default, and read/write when the Iceberg REST catalog requests it for a
 * native Iceberg table. A GCS location is used because that path returns a plain config map without
 * constructing any cloud client.
 */
public class TableConfigServiceTest {

  private final StorageCredentialVendor mockVendor = mock();
  private final TableMetadata mockMetadata = mock();
  private TableConfigService tableConfigService;

  @BeforeEach
  public void setUp() {
    ServerProperties serverProperties = mock();
    when(serverProperties.getS3Configurations()).thenReturn(Map.of());
    when(mockMetadata.location()).thenReturn("gs://test-bucket/table");
    when(mockVendor.vendCredential(any(), any()))
        .thenReturn(
            new TemporaryCredentials()
                .gcpOauthToken(new GcpOauthToken().oauthToken("token"))
                .expirationTime(0L));
    tableConfigService = new TableConfigService(mockVendor, serverProperties);
  }

  @Test
  public void defaultsToReadOnlyCredentials() {
    tableConfigService.getTableConfig(mockMetadata);
    verify(mockVendor).vendCredential(any(), eq(CredentialContext.READ_ONLY));
  }

  @Test
  public void passesThroughRequestedPrivileges() {
    tableConfigService.getTableConfig(mockMetadata, CredentialContext.READ_WRITE);
    verify(mockVendor).vendCredential(any(), eq(CredentialContext.READ_WRITE));
  }
}
