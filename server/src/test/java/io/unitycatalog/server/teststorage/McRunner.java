package io.unitycatalog.server.teststorage;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;

/**
 * Runs the MinIO client, {@code mc}, in a container of the MinIO image. Storage can answer its
 * health check before its admin API accepts connections, and a loaded machine can drop an admin
 * call, so a failed connection is retried for up to {@link #ADMIN_READY_TIMEOUT}; any other failure
 * is not.
 */
final class McRunner {
  private static final String MC = "/opt/bitnami/minio-client/bin/mc";
  private static final Duration ADMIN_READY_TIMEOUT = Duration.ofSeconds(60);
  private static final List<String> TRANSIENT_MC_ERRORS =
      List.of(
          "connection refused",
          "connection reset",
          "i/o timeout",
          "context canceled",
          "Client disconnected before response was ready");
  // What mc reports for a change the storage already has, as when a dropped call was applied.
  private static final String ALREADY_IN_EFFECT = "already in effect";

  private final GenericContainer<?> container;
  private final List<String> options;

  /**
   * @param container the container to run {@code mc} in
   * @param options options before each command, such as {@code --config-dir}
   */
  McRunner(GenericContainer<?> container, String... options) {
    this.container = container;
    this.options = List.of(options);
  }

  /**
   * @param args the {@code mc} command and its arguments
   * @throws IllegalStateException if the command fails
   */
  void run(String... args) {
    String[] command =
        Stream.of(Stream.of(MC), options.stream(), Stream.of(args))
            .flatMap(s -> s)
            .toArray(String[]::new);
    long deadline = System.nanoTime() + ADMIN_READY_TIMEOUT.toNanos();
    for (boolean isRetry = false; ; isRetry = true) {
      ExecResult result;
      try {
        result = container.execInContainer(command);
      } catch (IOException e) {
        throw new IllegalStateException("Could not run mc " + String.join(" ", args), e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted running mc " + String.join(" ", args), e);
      }
      String output = result.getStdout() + result.getStderr();
      if (result.getExitCode() == 0 || (isRetry && output.contains(ALREADY_IN_EFFECT))) {
        return;
      }
      if (!isTransient(output) || System.nanoTime() > deadline) {
        throw new IllegalStateException("mc " + String.join(" ", args) + " failed: " + output);
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted running mc " + String.join(" ", args), e);
      }
    }
  }

  /** Whether {@code mc} failed to reach the admin API, rather than being refused the change. */
  private static boolean isTransient(String output) {
    return TRANSIENT_MC_ERRORS.stream().anyMatch(output::contains);
  }
}
