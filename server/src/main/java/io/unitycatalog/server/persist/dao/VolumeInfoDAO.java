package io.unitycatalog.server.persist.dao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

import java.util.Date;

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
    @Column(name = "volume_id")
    private String volumeId;

    @Column(name = "name")
    private String name;
    @Column(name = "catalog_name")
    private String catalogName;
    @Column(name = "schema_name")
    private String schemaName;
    @Column(name = "comment")
    private String comment;
    @Column(name = "storage_location")
    private String storageLocation;
    @Column(name = "created_at")
    private Date createdAt;
    @Column(name = "updated_at")
    private Date updatedAt;
    @Column(name = "full_name")
    private String fullName;
    @Column(name = "volume_type")
    private String volumeType;

}
