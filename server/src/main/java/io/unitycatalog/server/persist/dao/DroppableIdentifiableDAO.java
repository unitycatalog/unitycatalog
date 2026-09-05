package io.unitycatalog.server.persist.dao;

import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.hibernate.annotations.ColumnDefault;

@MappedSuperclass
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class DroppableIdentifiableDAO extends IdentifiableDAO {
  @Column(name = "dropped_name")
  private String droppedName;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "dropped_at")
  private Date droppedAt;

  @ColumnDefault("0")
  @Column(name = "purge_state", nullable = false)
  private short purgeState;

  @ColumnDefault("0")
  @Column(name = "num_cleanup_retries", nullable = false)
  private short numCleanupRetries;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "last_cleanup_at")
  private Date lastCleanupAt;
}
