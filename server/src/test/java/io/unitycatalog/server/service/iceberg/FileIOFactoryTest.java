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
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link FileIOFactory} requests storage credentials with the privileges the caller
 * asks for: read-only by default, and read/write when the Iceberg REST catalog writes metadata for
 * a native Iceberg table (the old hard-coded SELECT-only behavior was the removed FIXME). A GCS
 * location is used because that path vends credentials eagerly while building the FileIO.
 */
public class FileIOFactoryTest {

  private static final NormalizedURL GCS_LOCATION = NormalizedURL.from("gs://test-bucket/table");

  private final StorageCredentialVendor mockVendor = mock();
  private FileIOFactory fileIOFactory;

  @BeforeEach
  public void setUp() {
    ServerProperties serverProperties = mock();
    when(serverProperties.getS3Configurations()).thenReturn(Map.of());
    when(mockVendor.vendCredential(any(), any()))
        .thenReturn(
            new TemporaryCredentials().gcpOauthToken(new GcpOauthToken().oauthToken("token")));
    fileIOFactory = new FileIOFactory(mockVendor, serverProperties);
  }

  @Test
  public void defaultsToReadOnlyCredentials() {
    fileIOFactory.getFileIO(GCS_LOCATION);
    verify(mockVendor).vendCredential(eq(GCS_LOCATION), eq(CredentialContext.READ_ONLY));
  }

  @Test
  public void passesThroughRequestedPrivileges() {
    fileIOFactory.getFileIO(GCS_LOCATION, CredentialContext.READ_WRITE);
    verify(mockVendor).vendCredential(eq(GCS_LOCATION), eq(CredentialContext.READ_WRITE));
  }
}
