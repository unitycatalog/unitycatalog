package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.model.VolumeType;
import io.unitycatalog.server.persist.utils.FileOperations;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import java.util.Date;
import java.util.UUID;
import lombok.*;
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

  public VolumeInfo toVolumeInfo() {
    return new VolumeInfo()
        .volumeId(getId().toString())
        .name(getName())
        .comment(comment)
        .storageLocation(FileOperations.convertRelativePathToURI(storageLocation))
        .owner(owner)
        .createdAt(createdAt.getTime())
        .createdBy(createdBy)
        .updatedAt(updatedAt != null ? updatedAt.getTime() : null)
        .updatedBy(updatedBy)
        .volumeType(VolumeType.valueOf(volumeType));
  }

  public static VolumeInfoDAO from(VolumeInfo volumeInfo) {
    if (volumeInfo == null) {
      return null;
    }
    return VolumeInfoDAO.builder()
        .id(UUID.fromString(volumeInfo.getVolumeId()))
        .name(volumeInfo.getName())
        .comment(volumeInfo.getComment())
        .storageLocation(volumeInfo.getStorageLocation())
        .owner(volumeInfo.getOwner())
        .createdAt(
            volumeInfo.getCreatedAt() != null ? new Date(volumeInfo.getCreatedAt()) : new Date())
        .createdBy(volumeInfo.getCreatedBy())
        .updatedAt(volumeInfo.getUpdatedAt() != null ? new Date(volumeInfo.getUpdatedAt()) : null)
        .updatedBy(volumeInfo.getUpdatedBy())
        .volumeType(volumeInfo.getVolumeType().getValue())
        .build();
  }
}
