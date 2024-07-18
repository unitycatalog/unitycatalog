package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.model.VolumeType;
import io.unitycatalog.server.persist.utils.FileUtils;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.Date;
import java.util.UUID;
import lombok.*;

@Entity
@Table(name = "uc_volumes")
// lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class VolumeInfoDAO {
  @Id
  @Column(name = "id")
  private UUID id;

  @Column(name = "name")
  private String name;

  @Column(name = "schema_id")
  private UUID schemaId;

  @Column(name = "comment")
  private String comment;

  @Column(name = "storage_location")
  private String storageLocation;

  @Column(name = "created_at")
  private Date createdAt;

  @Column(name = "updated_at")
  private Date updatedAt;

  @Column(name = "volume_type")
  private String volumeType;

  public VolumeInfo toVolumeInfo() {
    return new VolumeInfo()
        .volumeId(id.toString())
        .name(name)
        .comment(comment)
        .storageLocation(FileUtils.convertRelativePathToURI(storageLocation))
        .createdAt(createdAt.getTime())
        .updatedAt(updatedAt.getTime())
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
        .createdAt(
            volumeInfo.getCreatedAt() != null ? new Date(volumeInfo.getCreatedAt()) : new Date())
        .updatedAt(
            volumeInfo.getUpdatedAt() != null ? new Date(volumeInfo.getUpdatedAt()) : new Date())
        .volumeType(volumeInfo.getVolumeType().getValue())
        .build();
  }
}
