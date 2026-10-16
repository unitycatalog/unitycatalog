package io.unitycatalog.server.testdatabase;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.platform.launcher.core.LauncherFactory;

/**
 * Runs a JUnit launcher session in a child JVM, because the selection comes from the environment
 * and a JVM cannot change its own.
 */
class SelectionCheckTest {

  /** Opens and closes a launcher session, as the test runner does for each test class. */
  public static class OpenLauncherSession {
    public static void main(String[] args) {
      LauncherFactory.openSession().close();
    }
  }

  @Test
  void unknownValueStopsTheLauncherSession() throws Exception {
    ChildJvm child = openSessionWith("postgres");
    assertThat(child.exitCode).isNotZero();
    assertThat(child.output).contains("UC_TEST_DATABASE=postgres is not one of [H2, POSTGRES_18]");
  }

  @Test
  void supportedValueOpensTheLauncherSession() throws Exception {
    ChildJvm child = openSessionWith("H2");
    assertThat(child.exitCode).as(child.output).isZero();
  }

  private record ChildJvm(int exitCode, String output) {}

  private static ChildJvm openSessionWith(String value) throws Exception {
    String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    ProcessBuilder builder =
        new ProcessBuilder(
                java,
                "-cp",
                System.getProperty("java.class.path"),
                OpenLauncherSession.class.getName())
            .redirectErrorStream(true);
    builder.environment().put(SupportedDatabase.ENV, value);
    Process process = builder.start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(process.waitFor(60, TimeUnit.SECONDS)).isTrue();
    return new ChildJvm(process.exitValue(), output);
  }
}
