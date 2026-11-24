package io.unitycatalog.server.persist.dao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * An external staged-write directory that registration cannot adopt. A committed external table
 * replaces the reservation; an abandoned stage stays reserved because its writer can still write.
 */
@Entity
@Table(name = "uc_iceberg_write_reservations")
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class IcebergWriteReservationDAO extends IdentifiableDAO {
  @Column(name = "storage_location", length = 4096, nullable = false)
  private String storageLocation;
}
