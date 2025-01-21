package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.utils.FileOperations;
import jakarta.persistence.*;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import lombok.*;
import lombok.experimental.SuperBuilder;

// Hibernate annotations
@Entity
@Table(
    name = "uc_tables",
    indexes = {
      @Index(name = "idx_name", columnList = "name"),
    })
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
public class TableInfoDAO extends IdentifiableDAO {
  @Column(name = "schema_id")
  private UUID schemaId;

  @Column(name = "type")
  private String type;

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

  @Column(name = "data_source_format")
  private String dataSourceFormat;

  @Column(name = "comment", length = 65535)
  private String comment;

  @Column(name = "url", length = 2048)
  private String url;

  @Column(name = "column_count")
  private Integer columnCount;

  @OneToMany(
      mappedBy = "table",
      cascade = CascadeType.ALL,
      orphanRemoval = true,
      fetch = FetchType.LAZY)
  private List<ColumnInfoDAO> columns;

  @Column(name = "uniform_iceberg_metadata_location", length = 65535)
  private String uniformIcebergMetadataLocation;

  public static TableInfoDAO from(TableInfo tableInfo) {
    return TableInfoDAO.builder()
        .id(UUID.fromString(tableInfo.getTableId()))
        .name(tableInfo.getName())
        .comment(tableInfo.getComment())
        .owner(tableInfo.getOwner())
        .createdAt(
            tableInfo.getCreatedAt() != null ? new Date(tableInfo.getCreatedAt()) : new Date())
        .createdBy(tableInfo.getCreatedBy())
        .updatedAt(tableInfo.getUpdatedAt() != null ? new Date(tableInfo.getUpdatedAt()) : null)
        .updatedBy(tableInfo.getUpdatedBy())
        .columnCount(tableInfo.getColumns() != null ? tableInfo.getColumns().size() : 0)
        .url(tableInfo.getStorageLocation() != null ? tableInfo.getStorageLocation() : null)
        .type(tableInfo.getTableType().toString())
        .dataSourceFormat(tableInfo.getDataSourceFormat().toString())
        .url(tableInfo.getStorageLocation())
        .columns(ColumnInfoDAO.fromList(tableInfo.getColumns()))
        .build();
  }

  public TableInfo toTableInfo(boolean fetchColumns) {
    TableInfo tableInfo =
        new TableInfo()
            .tableId(getId().toString())
            .name(getName())
            .tableType(TableType.valueOf(type))
            .dataSourceFormat(DataSourceFormat.valueOf(dataSourceFormat))
            .storageLocation(FileOperations.convertRelativePathToURI(url))
            .comment(comment)
            .owner(owner)
            .createdAt(createdAt != null ? createdAt.getTime() : null)
            .createdBy(createdBy)
            .updatedAt(updatedAt != null ? updatedAt.getTime() : null)
            .updatedBy(updatedBy);
    if (fetchColumns) {
      tableInfo.columns(ColumnInfoDAO.toList(columns));
    }
    return tableInfo;
  }
}
