package io.unitycatalog.server.persist.dao;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.UuidGenerator;

import java.io.Serializable;
import java.util.UUID;

//Hibernate annotations
@Entity
@Table(name = "uc_properties", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"entity_id", "entity_type", "property_key"})})
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

}