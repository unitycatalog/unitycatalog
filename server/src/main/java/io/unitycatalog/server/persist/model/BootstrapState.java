package io.unitycatalog.server.persist.model;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Model representing bootstrap state for Azure OWNER initialization.
 *
 * <p>Tracks completion of one-time bootstrap process per metastore to prevent duplicate OWNER
 * assignments.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class BootstrapState {
  private String id;
  private String metastoreId;
  private String azureObjectId;
  private String principalEmail;
  private String userId;
  private Instant bootstrappedAt;
}
