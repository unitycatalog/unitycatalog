package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Builds the per-table FileIO configuration (credentials, region, etc.) returned to clients in the
 * Iceberg REST catalog's loadTable {@code config} field.
 */
public class TableConfigService {
  private final FileOperations fileOperations;

  public TableConfigService(FileOperations fileOperations) {
    this.fileOperations = fileOperations;
  }

  /**
   * Builds credentials from the location persisted in the UC table DAO. Do not accept a location
   * extracted from client-supplied Iceberg metadata here: that metadata is untrusted input.
   *
   * @param credentialsEndpoint path of this table's loadCredentials endpoint, relative to the
   *     catalog URI, so a client can renew the credentials before they expire
   */
  public Map<String, String> getTableConfig(
      NormalizedURL tableLocation,
      Set<CredentialContext.Privilege> privileges,
      String credentialsEndpoint) {
    return fileOperations.getFileIOConfig(
        tableLocation, privileges, Optional.of(credentialsEndpoint));
  }
}
