package io.unitycatalog.server.persist.dao;


import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.FileUtils;
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

    @OneToMany(mappedBy = "table", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    private List<ColumnInfoDAO> columns;

    @Column(name = "uniform_iceberg_metadata_location", columnDefinition = "TEXT")
    private String uniformIcebergMetadataLocation;

    @OneToMany(mappedBy = "table", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<PropertyDAO> properties;

    public static TableInfoDAO from(TableInfo tableInfo) {
        return TableInfoDAO.builder()
                .id(UUID.fromString(tableInfo.getTableId()))
                .name(tableInfo.getName())
                .comment(tableInfo.getComment())
                .createdAt(tableInfo.getCreatedAt() != null ? new Date(tableInfo.getCreatedAt()) : new Date())
                .updatedAt(tableInfo.getUpdatedAt() != null ? new Date(tableInfo.getUpdatedAt()) : null)
                .columnCount(tableInfo.getColumns() != null ? tableInfo.getColumns().size() : 0)
                .url(tableInfo.getStorageLocation() != null ? tableInfo.getStorageLocation() : null)
                .type(tableInfo.getTableType().toString())
                .dataSourceFormat(tableInfo.getDataSourceFormat().toString())
                .url(tableInfo.getStorageLocation())
                .columns(ColumnInfoDAO.fromList(tableInfo.getColumns()))
                .properties(PropertyDAO.from(tableInfo.getProperties()))
                .build();
    }

    public TableInfo toTableInfo() {
        return new TableInfo()
                .name(name)
                .tableType(TableType.valueOf(type))
                .dataSourceFormat(DataSourceFormat.valueOf(dataSourceFormat))
                .storageLocation(FileUtils.convertRelativePathToURI(url))
                .comment(comment)
                .createdAt(createdAt != null ? createdAt.getTime() : null)
                .updatedAt(updatedAt != null ? updatedAt.getTime() : null)
                .tableId(id.toString())
                .columns(ColumnInfoDAO.toList(columns))
                .properties(PropertyDAO.toMap(properties));
    }
}
