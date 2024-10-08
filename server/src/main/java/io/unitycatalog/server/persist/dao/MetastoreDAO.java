package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.GetMetastoreSummaryResponse;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import java.util.Date;
import lombok.*;
import lombok.experimental.SuperBuilder;

@Entity
@Table(name = "uc_metastore")
// Lombok
@Getter
@Setter
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class MetastoreDAO extends IdentifiableDAO {
  @Column(name = "created_at")
  private Date createdAt;

  @Column(name = "updated_at")
  private Date updatedAt;

  public GetMetastoreSummaryResponse toGetMetastoreSummaryResponse() {
    return new GetMetastoreSummaryResponse()
        .metastoreId(getId().toString())
        .name(getName())
        .createdAt(getCreatedAt() != null ? getCreatedAt().getTime() : null)
        .updatedAt(getUpdatedAt() != null ? getUpdatedAt().getTime() : null);
  }
}
