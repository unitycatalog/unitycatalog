package io.unitycatalog.server.service.iceberg;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link TableConfigService} vends storage credentials with the privileges the caller
 * asks for. A GCS location is used because that path returns a plain config map without
 * constructing any cloud client.
 */
public class TableConfigServiceTest {

  private final FileOperations mockFileOperations = mock();
  private TableConfigService tableConfigService;

  @BeforeEach
  public void setUp() {
    tableConfigService = new TableConfigService(mockFileOperations);
  }

  @Test
  public void passesThroughRequestedPrivileges() {
    tableConfigService.getTableConfig(
        NormalizedURL.from("gs://test-bucket/table"), CredentialContext.READ_WRITE);
    verify(mockFileOperations)
        .getFileIOConfig(any(NormalizedURL.class), eq(CredentialContext.READ_WRITE));
  }
}
