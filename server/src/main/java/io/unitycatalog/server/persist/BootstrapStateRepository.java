package io.unitycatalog.server.persist;

import io.unitycatalog.server.persist.dao.BootstrapStateDAO;
import io.unitycatalog.server.persist.model.BootstrapState;
import io.unitycatalog.server.persist.utils.TransactionManager;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repository for managing bootstrap state to ensure one-time OWNER bootstrap per metastore.
 *
 * <p>Tracks bootstrap completion and enforces sealed bootstrap window after server startup.
 */
public class BootstrapStateRepository {

  private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapStateRepository.class);
  private final SessionFactory sessionFactory;
  private static final long BOOTSTRAP_WINDOW_SECONDS = 300; // 5 minutes after server start
  private static final Instant SERVER_START_TIME = Instant.now();

  public BootstrapStateRepository(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  public Optional<BootstrapState> findByMetastoreId(String metastoreId) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          BootstrapStateDAO dao =
              session
                  .createQuery(
                      "FROM BootstrapStateDAO WHERE metastoreId = :metastoreId",
                      BootstrapStateDAO.class)
                  .setParameter("metastoreId", metastoreId)
                  .uniqueResult();
          return Optional.ofNullable(dao).map(BootstrapStateDAO::toBootstrapState);
        },
        "Failed to find bootstrap state",
        /* readOnly = */ true);
  }

  public BootstrapState create(
      String metastoreId, String azureObjectId, String principalEmail, String userId) {
    BootstrapState state =
        BootstrapState.builder()
            .id(UUID.randomUUID().toString())
            .metastoreId(metastoreId)
            .azureObjectId(azureObjectId)
            .principalEmail(principalEmail)
            .userId(userId)
            .bootstrappedAt(Instant.now())
            .build();

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          session.persist(BootstrapStateDAO.from(state));
          return state;
        },
        "Failed to create bootstrap state",
        /* readOnly = */ false);
  }

  public boolean isBootstrapWindowOpen() {
    // Bootstrap window remains open for BOOTSTRAP_WINDOW_SECONDS after server start
    Instant windowCloseTime = SERVER_START_TIME.plusSeconds(BOOTSTRAP_WINDOW_SECONDS);
    boolean isOpen = Instant.now().isBefore(windowCloseTime);

    LOGGER.debug(
        "Bootstrap window check: open={}, serverStart={}, windowClose={}",
        isOpen,
        SERVER_START_TIME,
        windowCloseTime);

    return isOpen;
  }
}
