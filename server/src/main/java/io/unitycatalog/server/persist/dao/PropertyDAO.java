package io.unitycatalog.server.persist.dao;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.UuidGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;


@Entity
@Table(name = "uc_properties")
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

    @Column(name = "property_key", nullable = false)
    private String key;

    @Column(name = "property_value", nullable = false)
    private String value;

    @ManyToOne
    @JoinColumn(name = "catalog_id", referencedColumnName = "id")
    private CatalogInfoDAO catalog;

    @ManyToOne
    @JoinColumn(name = "schema_id", referencedColumnName = "id")
    private SchemaInfoDAO schema;

    @ManyToOne
    @JoinColumn(name = "table_id", referencedColumnName = "id")
    private TableInfoDAO table;

    public static List<PropertyDAO> from(Map<String, String> properties) {
        if (properties == null) {
            return new ArrayList<>();
        }
        return properties.entrySet().stream()
                .map(entry -> PropertyDAO.builder()
                        .key(entry.getKey())
                        .value(entry.getValue())
                        .build())
                .collect(Collectors.toList());
    }

    public static Map<String, String> toMap(List<PropertyDAO> propertyDAOList) {
        return propertyDAOList.stream()
                .collect(Collectors.toMap(PropertyDAO::getKey, PropertyDAO::getValue));
    }
}