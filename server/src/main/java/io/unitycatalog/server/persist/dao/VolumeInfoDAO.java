package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.model.VolumeType;
import io.unitycatalog.server.utils.NormalizedURL;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import java.util.Date;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@Entity
@Table(name = "uc_volumes")
// lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class VolumeInfoDAO extends IdentifiableDAO {
  @Column(name = "schema_id")
  private UUID schemaId;

  @Column(name = "comment")
  private String comment;

  @Column(name = "storage_location")
  private String storageLocation;

  @Column(name = "owner")
  private String owner;

  @Column(name = "created_at")
  private Date createdAt;

  @Column(name = "created_by")
  private String createdBy;

  @Column(name = "updated_at")
  private Date updatedAt;

  @Column(name = "updated_by")
  private String updatedBy;

  @Column(name = "volume_type")
  private String volumeType;

  public VolumeInfo toVolumeInfo(String catalogName, String schemaName) {
    return new VolumeInfo()
        .volumeId(getId().toString())
        .name(getName())
        .catalogName(catalogName)
        .schemaName(schemaName)
        .fullName(catalogName + "." + schemaName + "." + getName())
        .comment(comment)
        .storageLocation(NormalizedURL.normalize(storageLocation))
        .owner(owner)
        .createdAt(createdAt.getTime())
        .createdBy(createdBy)
        .updatedAt(updatedAt != null ? updatedAt.getTime() : null)
        .updatedBy(updatedBy)
        .volumeType(VolumeType.valueOf(volumeType));
  }
}
