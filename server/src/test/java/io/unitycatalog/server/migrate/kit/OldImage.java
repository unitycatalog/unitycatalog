package io.unitycatalog.server.migrate.kit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;

/**
 * A server image an {@link UpgradePath} runs: a published release, or one built from a git ref.
 * Resolved on first use, so listing the paths builds nothing.
 */
public final class OldImage {

  private final String description;
  private final Supplier<String> resolver;
  private String name;

  private OldImage(String description, Supplier<String> resolver) {
    this.description = description;
    this.resolver = resolver;
  }

  /** An image from a registry, for example {@code unitycatalog/unitycatalog:v0.7.0}. */
  public static OldImage published(String name) {
    return new OldImage(name, () -> name);
  }

  /**
   * The server built from {@code ref} by {@code dev/build-image.sh}, which reuses the image when
   * one for the ref's commit exists.
   */
  public static OldImage builtFrom(String ref) {
    return new OldImage("built from " + ref, () -> build(ref));
  }

  /** The image name, building the image first if needed. */
  public synchronized String name() {
    if (name == null) {
      name = resolver.get();
    }
    return name;
  }

  @Override
  public String toString() {
    return description;
  }

  private static String build(String ref) {
    Path repo = Path.of(run(List.of("git", "rev-parse", "--show-toplevel")).trim());
    String output = run(List.of(repo.resolve("dev/build-image.sh").toString(), ref));
    List<String> lines = output.lines().toList();
    return lines.get(lines.size() - 1).trim();
  }

  /** Runs {@code command}, returning its standard output; its errors go to this process's. */
  private static String run(List<String> command) {
    try {
      Process process =
          new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
      String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      int exit = process.waitFor();
      if (exit != 0) {
        throw new IllegalStateException(String.join(" ", command) + " exited with " + exit);
      }
      return output;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
