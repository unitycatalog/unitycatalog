package io.unitycatalog.server.persist.dao;

import jakarta.persistence.*;

import org.hibernate.annotations.UuidGenerator;

import java.io.Serializable;
import java.util.UUID;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

//Hibernate annotations
@Entity
@Table(name = "uc_columns", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"table_id", "ordinal_position" , "name"})
})
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@Builder
public class ColumnInfoDAO {

    @Id
    @UuidGenerator
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @ManyToOne
    @JoinColumn(name = "table_id", nullable = false)
    private TableInfoDAO tableId;

    @Column(name = "ordinal_position", nullable = false)
    private short ordinalPosition;

    @Column(name = "name", nullable = false, length = 255)
    private String name;

    @Lob
    @Column(name = "type_text", nullable = false, columnDefinition = "MEDIUMTEXT")
    private String typeText;

    @Lob
    @Column(name = "type_json", nullable = false, columnDefinition = "MEDIUMTEXT")
    private String typeJson;

    @Column(name = "type_name", nullable = false, length = 32)
    private String typeName;

    @Column(name = "type_precision")
    private Integer typePrecision;

    @Column(name = "type_scale")
    private Integer typeScale;

    @Column(name = "type_interval_type", length = 255)
    private String typeIntervalType;

    @Column(name = "nullable", nullable = false)
    private boolean nullable;

    @Lob
    @Column(name = "comment")
    private String comment;

    @Column(name = "partition_index")
    private Short partitionIndex;

}
