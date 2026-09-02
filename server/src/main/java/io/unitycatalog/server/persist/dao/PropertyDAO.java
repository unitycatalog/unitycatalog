package io.unitycatalog.server.persist.dao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.annotations.UuidGenerator;

// Hibernate annotations
@Entity
@Table(
    name = "uc_properties",
    uniqueConstraints = {
      @UniqueConstraint(columnNames = {"entity_id", "entity_type", "property_key"})
    })
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@Builder
public class PropertyDAO {
  @Id
  @UuidGenerator
  @Column(name = "id", updatable = false, nullable = false)
  private UUID id;

  @Column(name = "entity_id", nullable = false)
  private UUID entityId;

  @Column(name = "entity_type", nullable = false)
  private String entityType;

  @Column(name = "property_key", nullable = false)
  private String key;

  /**
   * Entity property values, including Spark datasource schema JSON ({@code
   * spark.sql.sources.schema.part.N}).
   *
   * <p>An unannotated {@code String} is mapped as {@code varchar(255)}. That is too small for Spark
   * table properties, so the length matches {@code ColumnInfoDAO.typeJson}.
   *
   * <p>{@code hibernate.hbm2ddl.auto=update} does not change the type of an existing column. Fresh
   * databases pick up this mapping; existing deployments must {@code ALTER} {@code
   * uc_properties.property_value} as described in {@code docs/server/deployment.md}.
   */
  @Column(name = "property_value", nullable = false, length = 16777215)
  private String value;

  public static List<PropertyDAO> from(
      Map<String, String> properties, UUID entityId, String entityType) {
    if (properties == null) {
      return new ArrayList<>();
    }
    return properties.entrySet().stream()
        .map(
            entry ->
                PropertyDAO.builder()
                    .key(entry.getKey())
                    .value(entry.getValue())
                    .entityId(entityId)
                    .entityType(entityType)
                    .build())
        .collect(Collectors.toList());
  }

  public static Map<String, String> toMap(List<PropertyDAO> propertyDAOList) {
    return propertyDAOList.stream()
        .collect(Collectors.toMap(PropertyDAO::getKey, PropertyDAO::getValue));
  }
}
