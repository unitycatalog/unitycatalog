package io.unitycatalog.server.persist.dao;

import jakarta.persistence.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.*;
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

  @Column(name = "property_value", nullable = false)
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
