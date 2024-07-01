package io.unitycatalog.server.persist.dao;

import jakarta.persistence.*;

import lombok.*;

import java.util.Date;
import java.util.List;
import java.util.UUID;


// Hibernate annotations
@Entity
@Table(name = "uc_tables" , indexes = {
        @Index(name = "idx_updated_at", columnList = "updated_at"),
})
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Builder
public class TableInfoDAO {

    @Id
    @Column(name = "id", columnDefinition = "BINARY(16)")
    private UUID id;

    @Column(name = "schema_id", columnDefinition = "BINARY(16)")
    private UUID schemaId;

    @Column(name = "name")
    private String name;

    @Column(name = "type")
    private String type;

    @Column(name = "created_at")
    private Date createdAt;

    @Column(name = "updated_at")
    private Date updatedAt;

    @Column(name = "data_source_format")
    private String dataSourceFormat;

    @Column(name = "comment", columnDefinition = "TEXT")
    private String comment;

    @Column(name = "url", length = 2048)
    private String url;

    @Column(name = "column_count")
    private Integer columnCount;

    @OneToMany(mappedBy = "tableId", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<ColumnInfoDAO> columns;

    @Column(name = "uniform_iceberg_metadata_location", columnDefinition = "TEXT")
    private String uniformIcebergMetadataLocation;

}
