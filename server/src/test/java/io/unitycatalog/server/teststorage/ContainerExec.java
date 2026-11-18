package io.unitycatalog.server.teststorage;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;

/**
 * Runs a setup command in a storage container with a deadline, so a hung command fails the start
 * rather than holding every later suite behind {@link TestStorages#selected()}.
 */
final class ContainerExec {
  private ContainerExec() {}

  /**
   * @param container the container to run the command in
   * @param timeout how long the command may take
   * @param command the command and its arguments
   * @return the command's standard output
   * @throws IllegalStateException if the command cannot run, fails, or takes longer than {@code
   *     timeout}
   */
  static String run(GenericContainer<?> container, Duration timeout, String... command) {
    String commandLine = String.join(" ", command);
    FutureTask<ExecResult> exec =
        new FutureTask<>(() -> container.execInContainer(StandardCharsets.UTF_8, command));
    // A daemon thread, so a command that never returns does not keep the test JVM alive; stopping
    // the container after the failed start ends it.
    Thread thread = new Thread(exec, "container-exec");
    thread.setDaemon(true);
    thread.start();
    ExecResult result;
    try {
      result = exec.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      thread.interrupt();
      throw new IllegalStateException(commandLine + " did not finish within " + timeout, e);
    } catch (ExecutionException e) {
      throw new IllegalStateException("Could not run " + commandLine, e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted running " + commandLine, e);
    }
    if (result.getExitCode() != 0) {
      throw new IllegalStateException(
          commandLine + " failed: " + result.getStdout() + result.getStderr());
    }
    return result.getStdout();
  }
}
