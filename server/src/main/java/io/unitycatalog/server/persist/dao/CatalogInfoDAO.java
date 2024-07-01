package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.CatalogInfo;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;

@Entity
@Table(name = "uc_catalogs")
//Lombok
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CatalogInfoDAO {

    @Id
    @Column(name = "id", columnDefinition = "BINARY(16)")
    private UUID id;

    @Column(name = "name")
    private String name;

    @Column(name = "comment")
    private String comment;

    @Column(name = "created_at")
    private Date createdAt;

    @Column(name = "updated_at")
    private Date updatedAt;

    public static CatalogInfoDAO toCatalogInfoDAO(CatalogInfo catalogInfo) {
        return CatalogInfoDAO.builder()
            .id(catalogInfo.getId() != null ? UUID.fromString(catalogInfo.getId()) : null)
            .name(catalogInfo.getName())
            .comment(catalogInfo.getComment())
            .createdAt(catalogInfo.getCreatedAt() != null ? Date.from(Instant
                    .ofEpochMilli(catalogInfo.getCreatedAt())) : new Date())
            .updatedAt(catalogInfo.getUpdatedAt() != null ? Date.from(Instant
                    .ofEpochMilli(catalogInfo.getUpdatedAt())) : new Date())
            .build();
    }

    public static CatalogInfo toCatalogInfo(CatalogInfoDAO catalogInfoDAO) {
        return new CatalogInfo()
            .id(catalogInfoDAO.getId().toString())
            .name(catalogInfoDAO.getName())
            .comment(catalogInfoDAO.getComment())
            .createdAt(catalogInfoDAO.getCreatedAt().getTime())
            .updatedAt(catalogInfoDAO.getUpdatedAt() != null? catalogInfoDAO.getUpdatedAt().getTime() :null);
    }


}
