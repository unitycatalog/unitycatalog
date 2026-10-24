package io.unitycatalog.server.migrate.kit.slowstep;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

/**
 * A test-only step that stays running until {@link #release()} so a test can start a second server
 * while the first is still setting up the schema. Does nothing to the database.
 */
public class V1000__Slow_test_step extends BaseJavaMigration {

  private static volatile CountDownLatch started = new CountDownLatch(1);
  private static volatile CountDownLatch released = new CountDownLatch(1);

  /** Call before a test, so the latches are fresh. */
  public static void reset() {
    started = new CountDownLatch(1);
    released = new CountDownLatch(1);
  }

  public static boolean awaitStarted() throws InterruptedException {
    return started.await(2, TimeUnit.MINUTES);
  }

  public static void release() {
    released.countDown();
  }

  @Override
  public void migrate(Context context) throws Exception {
    started.countDown();
    if (!released.await(2, TimeUnit.MINUTES)) {
      throw new IllegalStateException("The test never released the slow step");
    }
  }
}
