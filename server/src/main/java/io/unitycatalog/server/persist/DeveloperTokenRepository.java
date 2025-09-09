package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.DeveloperTokenDAO;
import io.unitycatalog.server.persist.utils.TransactionManager;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeveloperTokenRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeveloperTokenRepository.class);
  private final SessionFactory sessionFactory;

  public DeveloperTokenRepository(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  public DeveloperTokenDAO createToken(DeveloperTokenDAO tokenDAO) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          session.persist(tokenDAO);
          session.flush();
          return tokenDAO;
        },
        "Failed to create developer token",
        false);
  }

  public List<DeveloperTokenDAO> listTokensByUser(String userId) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          Query<DeveloperTokenDAO> query =
              session.createQuery(
                  "FROM DeveloperTokenDAO WHERE userId = :userId ORDER BY creationTime DESC",
                  DeveloperTokenDAO.class);
          query.setParameter("userId", userId);
          return query.getResultList();
        },
        "Failed to list developer tokens by user",
        true);
  }

  public Optional<DeveloperTokenDAO> getTokenByIdAndUser(String tokenId, String userId) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          Query<DeveloperTokenDAO> query =
              session.createQuery(
                  "FROM DeveloperTokenDAO WHERE id = :tokenId AND userId = :userId",
                  DeveloperTokenDAO.class);
          query.setParameter("tokenId", tokenId);
          query.setParameter("userId", userId);
          return query.uniqueResultOptional();
        },
        "Failed to get developer token by ID and user",
        true);
  }

  public Optional<DeveloperTokenDAO> getTokenByHash(String tokenHash) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          Query<DeveloperTokenDAO> query =
              session.createQuery(
                  "FROM DeveloperTokenDAO WHERE tokenHash = :tokenHash AND status = :status",
                  DeveloperTokenDAO.class);
          query.setParameter("tokenHash", tokenHash);
          query.setParameter("status", DeveloperTokenDAO.TokenStatus.ACTIVE);
          return query.uniqueResultOptional();
        },
        "Failed to get developer token by hash",
        true);
  }

  public void revokeToken(String tokenId, String userId) {
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          Query<DeveloperTokenDAO> query =
              session.createQuery(
                  "FROM DeveloperTokenDAO WHERE id = :tokenId AND userId = :userId",
                  DeveloperTokenDAO.class);
          query.setParameter("tokenId", tokenId);
          query.setParameter("userId", userId);

          Optional<DeveloperTokenDAO> tokenOpt = query.uniqueResultOptional();
          if (tokenOpt.isPresent()) {
            DeveloperTokenDAO token = tokenOpt.get();
            token.setStatus(DeveloperTokenDAO.TokenStatus.REVOKED);
            session.merge(token);
          } else {
            throw new BaseException(ErrorCode.NOT_FOUND, "Developer token not found");
          }
          return null;
        },
        "Failed to revoke developer token",
        false);
  }

  public void cleanupExpiredTokens() {
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          long currentTime = System.currentTimeMillis();
          Query<?> query =
              session.createQuery(
                  "UPDATE DeveloperTokenDAO SET status = :revoked WHERE expiryTime < :currentTime AND status = :active");
          query.setParameter("revoked", DeveloperTokenDAO.TokenStatus.REVOKED);
          query.setParameter("currentTime", new Date(currentTime));
          query.setParameter("active", DeveloperTokenDAO.TokenStatus.ACTIVE);
          int updated = query.executeUpdate();
          LOGGER.info("Marked {} expired developer tokens as revoked", updated);
          return null;
        },
        "Failed to cleanup expired developer tokens",
        false);
  }
}
