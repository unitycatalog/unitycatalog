package io.unitycatalog.server.persist.dao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.util.Date;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * A revoked access token, keyed by its JWT ID (jti). A row is written when a session is logged out,
 * and is consulted on every authenticated request so the token is no longer accepted. Storing the
 * denylist in the database means a revoked token is rejected on its next request without relying on
 * any in-memory state.
 *
 * <p>Rows are retained only until {@link #expiresAt}: once a token would expire on its own there is
 * nothing left to revoke, so expired rows are purged once their expiry has passed. A {@code null}
 * expiry marks a token that never expires, so its revocation is permanent and the row is kept
 * indefinitely. The table therefore stays bounded by the number of not-yet-expired revocations.
 */
@Entity
@Table(
    name = "uc_token_revocations",
    // Index the expiry so the periodic cleanup delete is a range scan rather than a full scan.
    indexes = {@Index(name = "idx_token_revocations_expires_at", columnList = "expires_at")})
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Builder
public class TokenRevocationDAO {
  // jti is the token's JWT ID. This server always mints it as a random UUID, and the denylist only
  // ever holds tokens it issued, so it is stored as a native UUID (compact primary key, consistent
  // with other DAO ids).
  @Id
  @Column(name = "jti")
  private UUID jti;

  // When null, the revoked token has no expiry, so the revocation is permanent: the cleanup delete
  // (expires_at < now) never matches a null row, so it is kept indefinitely.
  @Column(name = "expires_at")
  private Date expiresAt;
}
