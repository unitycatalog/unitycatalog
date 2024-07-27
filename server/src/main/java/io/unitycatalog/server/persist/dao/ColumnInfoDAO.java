package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.ColumnTypeName;
import jakarta.persistence.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.*;
import lombok.experimental.SuperBuilder;

// Hibernate annotations
@Entity
@Table(
    name = "uc_columns",
    uniqueConstraints = {@UniqueConstraint(columnNames = {"table_id", "ordinal_position", "name"})})
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@ToString
@SuperBuilder
public class ColumnInfoDAO extends IdentifiableDAO {
  @ManyToOne
  @JoinColumn(name = "table_id", nullable = false, referencedColumnName = "id")
  private TableInfoDAO table;

  @Column(name = "ordinal_position", nullable = false)
  private short ordinalPosition;

  @Lob
  @Column(name = "type_text", nullable = false, length = 16777215)
  private String typeText;

  @Column(name = "type_json", nullable = false, length = 16777215)
  private String typeJson;

  @Column(name = "type_name", nullable = false, length = 32)
  private String typeName;

  @Column(name = "type_precision")
  private Integer typePrecision;

  @Column(name = "type_scale")
  private Integer typeScale;

  @Column(name = "type_interval_type")
  private String typeIntervalType;

  @Column(name = "nullable", nullable = false)
  private boolean nullable;

  @Column(name = "comment", length = 65535)
  private String comment;

  @Column(name = "partition_index")
  private Short partitionIndex;

  public static ColumnInfoDAO from(ColumnInfo column) {
    if (column == null) {
      return null;
    }
    return ColumnInfoDAO.builder()
        .name(column.getName())
        .typeText(column.getTypeText())
        .typeJson(column.getTypeJson())
        .typeName(column.getTypeName().toString())
        .typePrecision(column.getTypePrecision())
        .typeScale(column.getTypeScale())
        .typeIntervalType(column.getTypeIntervalType())
        .ordinalPosition(column.getPosition().shortValue())
        .comment(column.getComment())
        .nullable(Optional.ofNullable(column.getNullable()).orElse(false))
        .partitionIndex(
            column.getPartitionIndex() != null ? column.getPartitionIndex().shortValue() : null)
        .build();
  }

  public ColumnInfo toColumnInfo() {
    return new ColumnInfo()
        .name(getName())
        .typeText(typeText)
        .typeJson(typeJson)
        .typeName(ColumnTypeName.valueOf(typeName))
        .typePrecision(typePrecision)
        .typeScale(typeScale)
        .typeIntervalType(typeIntervalType)
        .position((int) ordinalPosition)
        .comment(comment)
        .nullable(nullable)
        .partitionIndex(partitionIndex != null ? partitionIndex.intValue() : null);
  }

  public static List<ColumnInfo> toList(List<ColumnInfoDAO> columnInfoDAOs) {
    if (columnInfoDAOs == null) {
      return new ArrayList<>();
    }
    return columnInfoDAOs.stream().map(ColumnInfoDAO::toColumnInfo).collect(Collectors.toList());
  }

  public static List<ColumnInfoDAO> fromList(List<ColumnInfo> columnInfos) {
    if (columnInfos == null) {
      return new ArrayList<>();
    }
    return columnInfos.stream().map(ColumnInfoDAO::from).collect(Collectors.toList());
  }
}
