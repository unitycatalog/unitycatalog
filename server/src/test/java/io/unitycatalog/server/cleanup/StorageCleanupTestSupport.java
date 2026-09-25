package io.unitycatalog.server.cleanup;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.hibernate.SessionFactory;

/** Shared helpers for storage-cleanup tests across the repository, worker, and SDK suites. */
public final class StorageCleanupTestSupport {
  private StorageCleanupTestSupport() {}

  /** Fetches the cleanup task keyed by its resource id, or {@code null} if none is queued. */
  public static StorageCleanupTaskDAO findTask(SessionFactory sessionFactory, UUID resourceId) {
    try (var session = sessionFactory.openSession()) {
      return session.get(StorageCleanupTaskDAO.class, resourceId);
    }
  }

  /** Returns every queued cleanup task; used to assert exactly-one-task cardinality. */
  public static List<StorageCleanupTaskDAO> allTasks(SessionFactory sessionFactory) {
    try (var session = sessionFactory.openSession()) {
      return session.createQuery("FROM StorageCleanupTaskDAO", StorageCleanupTaskDAO.class).list();
    }
  }

  /**
   * Polls until every path is gone and the task is cleared, then asserts the same once the timeout
   * elapses so a stuck cleanup fails with a clear message.
   */
  public static void awaitCleanup(
      SessionFactory sessionFactory, UUID resourceId, Duration timeout, Path... paths)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (allGone(paths) && findTask(sessionFactory, resourceId) == null) {
        return;
      }
      Thread.sleep(10);
    }
    for (Path path : paths) {
      assertThat(path).doesNotExist();
    }
    assertThat(findTask(sessionFactory, resourceId)).isNull();
  }

  /** Shrinks the poll interval and initial delay so a live worker reclaims storage quickly. */
  public static void configureFastCleanup(Properties serverProperties) {
    serverProperties.setProperty(Property.STORAGE_CLEANUP_POLL_INTERVAL.getKey(), "PT0.01S");
    serverProperties.setProperty(Property.STORAGE_CLEANUP_INITIAL_DELAY.getKey(), "PT0.001S");
  }

  private static boolean allGone(Path... paths) {
    for (Path path : paths) {
      if (Files.exists(path)) {
        return false;
      }
    }
    return true;
  }
}
