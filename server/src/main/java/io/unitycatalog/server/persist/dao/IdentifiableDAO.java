package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import jakarta.persistence.Column;
import jakarta.persistence.Id;
import jakarta.persistence.MappedSuperclass;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.Optional;
import java.util.UUID;

@MappedSuperclass
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class IdentifiableDAO {
    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "name", nullable = false)
    private String name;

    public static <T extends IdentifiableDAO> Optional<String> getParentIdColumnName(Class<T> entityClass) {
        if (TableInfoDAO.class == entityClass
            || VolumeInfoDAO.class == entityClass
            || FunctionInfoDAO.class == entityClass) {
            return Optional.of("schemaId");
        }
        if (SchemaInfoDAO.class == entityClass) {
            return Optional.of("catalogId");
        }
        return Optional.empty();
    }
}
