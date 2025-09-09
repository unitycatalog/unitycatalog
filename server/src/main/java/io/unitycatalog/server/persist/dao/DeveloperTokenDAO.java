package io.unitycatalog.server.persist.dao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.Date;
import java.util.UUID;
import lombok.*;
import lombok.experimental.SuperBuilder;

@Entity
@Table(name = "uc_developer_tokens")
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class DeveloperTokenDAO {

  @Id
  @Column(name = "id")
  private UUID id;

  @Column(name = "user_id", nullable = false)
  private String userId;

  @Column(name = "comment", nullable = false)
  private String comment;

  @Column(name = "token_hash", nullable = false, unique = true)
  private String tokenHash;

  @Column(name = "creation_time", nullable = false)
  private Date creationTime;

  @Column(name = "expiry_time", nullable = false)
  private Date expiryTime;

  @Column(name = "status", nullable = false)
  @Enumerated(EnumType.STRING)
  private TokenStatus status;

  public enum TokenStatus {
    ACTIVE,
    REVOKED
  }
}
