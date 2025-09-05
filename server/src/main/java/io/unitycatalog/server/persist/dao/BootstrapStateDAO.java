package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.persist.model.BootstrapState;
import jakarta.persistence.*;
import java.time.Instant;
import lombok.Data;
import lombok.NoArgsConstructor;

/** DAO for bootstrap_state table tracking Azure OWNER bootstrap completion. */
@Entity
@Table(name = "bootstrap_state")
@Data
@NoArgsConstructor
public class BootstrapStateDAO {

  @Id
  @Column(name = "id", nullable = false)
  private String id;

  @Column(name = "metastore_id", nullable = false, unique = true)
  private String metastoreId;

  @Column(name = "azure_object_id", nullable = false)
  private String azureObjectId;

  @Column(name = "principal_email", nullable = false)
  private String principalEmail;

  @Column(name = "user_id", nullable = false)
  private String userId;

  @Column(name = "bootstrapped_at", nullable = false)
  private Instant bootstrappedAt;

  public static BootstrapStateDAO from(BootstrapState state) {
    BootstrapStateDAO dao = new BootstrapStateDAO();
    dao.setId(state.getId());
    dao.setMetastoreId(state.getMetastoreId());
    dao.setAzureObjectId(state.getAzureObjectId());
    dao.setPrincipalEmail(state.getPrincipalEmail());
    dao.setUserId(state.getUserId());
    dao.setBootstrappedAt(state.getBootstrappedAt());
    return dao;
  }

  public BootstrapState toBootstrapState() {
    return BootstrapState.builder()
        .id(this.getId())
        .metastoreId(this.getMetastoreId())
        .azureObjectId(this.getAzureObjectId())
        .principalEmail(this.getPrincipalEmail())
        .userId(this.getUserId())
        .bootstrappedAt(this.getBootstrappedAt())
        .build();
  }
}
