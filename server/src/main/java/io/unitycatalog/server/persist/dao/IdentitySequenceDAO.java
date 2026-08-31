package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.IdentitySequenceInfo;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import java.io.Serializable;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * A catalog-hosted monotonic identity sequence, the counter behind a concurrent identity column.
 * Values are issued following {@code start + k * step}. The {@code allocationFrontier} is the last
 * value issued so far, sometimes also called the high-water mark. ({@code null} means nothing has
 * been issued yet, so the next reserve starts at {@code start}).
 */
@Entity
@Table(name = "uc_identity_sequences")
@IdClass(IdentitySequenceDAO.PrimaryKey.class)
// Lombok
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode
public class IdentitySequenceDAO {
  @Id
  @Column(name = "table_id")
  private String tableId;

  @Id
  @Column(name = "sequence_id")
  private String sequenceId;

  // "start" is a reserved word in several SQL dialects, so the column is named start_value.
  @Column(name = "start_value", nullable = false)
  private Long startValue;

  @Column(name = "step", nullable = false)
  private Long step;

  // The allocation frontier: the last value issued (a.k.a. the high-water mark). Null until the
  // first reservation, so a cold sequence issues start.
  @Column(name = "allocation_frontier")
  private Long allocationFrontier;

  // false = live; true = soft-deleted (retains state, rejects reservations until reactivated).
  @Column(name = "deleted", nullable = false)
  private boolean deleted;

  @Column(name = "created_at")
  private Date createdAt;

  @Column(name = "updated_at")
  private Date updatedAt;

  public IdentitySequenceInfo toIdentitySequenceInfo() {
    return new IdentitySequenceInfo()
        .sequenceId(sequenceId)
        .tableId(tableId)
        .start(startValue)
        .step(step);
  }

  /** The composite primary key of a sequence is {@code (table_id, sequence_id)}. */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class PrimaryKey implements Serializable {
    private String tableId;
    private String sequenceId;
  }
}
