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
    @JoinColumn(name = "catalog_id", nullable = false, referencedColumnName = "id")
    private CatalogInfoDAO catalog;

    @ManyToOne
    @JoinColumn(name = "schema_id", nullable = false, referencedColumnName = "id")
    private SchemaInfoDAO schema;

    @ManyToOne
    @JoinColumn(name = "table_id", nullable = false, referencedColumnName = "id")
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

/*
@Entity
@Table(name = "uc_catalog_properties", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"catalog_id", "property_key"})})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
//@Builder
class CatalogPropertyDAO extends PropertyDAO {
    @ManyToOne
    @JoinColumn(name = "catalog_id", nullable = false, referencedColumnName = "id")
    private CatalogInfoDAO catalog;

    public static List<CatalogPropertyDAO> from(Map<String, String> properties) {
        if (properties == null) {
            return new ArrayList<>();
        }
        return properties.entrySet().stream()
                .map(entry -> CatalogPropertyDAO.builder()
                        .key(entry.getKey())
                        .value(entry.getValue())
                        .build())
                .collect(Collectors.toList());
    }
}

@Entity
@Table(name = "uc_schema_properties", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"schema_id", "property_key"})})
class SchemaPropertyDAO extends PropertyDAO {
    @ManyToOne
    @JoinColumn(name = "schema_id", nullable = false, referencedColumnName = "id")
    private SchemaInfoDAO schema;
}

@Entity
@Table(name = "uc_table_properties", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"table_id", "property_key"})})
class TablePropertyDAO extends PropertyDAO {
    @ManyToOne
    @JoinColumn(name = "table_id", nullable = false, referencedColumnName = "id")
    private TableInfoDAO table;
}
*/