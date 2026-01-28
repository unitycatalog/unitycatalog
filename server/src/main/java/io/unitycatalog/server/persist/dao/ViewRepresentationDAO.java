package io.unitycatalog.server.persist.dao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Lob;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.hibernate.annotations.UuidGenerator;

// Hibernate annotations
@Entity
@Table(name = "uc_view_representation")
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@SuperBuilder
public class ViewRepresentationDAO {

  @Id
  @UuidGenerator
  @Column(name = "id", columnDefinition = "BINARY(16)")
  private UUID id;

  @ManyToOne
  @JoinColumn(name = "table_id", nullable = false, referencedColumnName = "id")
  private TableInfoDAO table;

  @Column(name = "dialect", nullable = false)
  private String dialect;

  @Lob
  @Column(name = "sql", nullable = false)
  private String sql;
}
