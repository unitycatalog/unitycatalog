package io.unitycatalog.server.testdatabase;

import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;

/**
 * Rejects an unknown {@value SupportedDatabase#ENV} before any test runs, so tests that never touch
 * the database fail too instead of passing on a misconfigured run. jupiter-interface opens one
 * launcher session per test class, so this runs once per class.
 */
public class SelectionCheck implements LauncherSessionListener {
  @Override
  public void launcherSessionOpened(LauncherSession session) {
    SupportedDatabase.selected();
  }
}
