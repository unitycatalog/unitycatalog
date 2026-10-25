package io.unitycatalog.server.migrate;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * An old server an {@link UpgradePath} runs: the shaded server jar of a git ref. Resolved on first
 * use, so listing the paths builds nothing.
 */
final class OldImage {

  private static final Path JAR_CACHE =
      Path.of(System.getProperty("user.home"), ".cache", "uc-upgrade-jars");

  private final String ref;
  private String name;

  private OldImage(String ref) {
    this.ref = ref;
  }

  /**
   * The shaded server jar of {@code ref}'s commit, built once into {@code
   * ~/.cache/uc-upgrade-jars}.
   */
  static OldImage jarBuiltFrom(String ref) {
    return new OldImage(ref);
  }

  /** The jar's path, building the jar first if needed. */
  synchronized String name() {
    if (name == null) {
      name = jar(ref);
    }
    return name;
  }

  @Override
  public String toString() {
    return ref;
  }

  private static String jar(String ref) {
    Path repo = Path.of(git("rev-parse", "--show-toplevel"));
    String sha = git("-C", repo.toString(), "rev-parse", ref + "^{commit}");
    Path cached = JAR_CACHE.resolve(sha + ".jar");
    if (!Files.exists(cached)) {
      build(repo, sha, cached);
    }
    return cached.toString();
  }

  /**
   * Exports {@code sha}'s tree with {@code git archive} (nothing untracked), builds its shaded
   * server jar there and copies it to {@code target}.
   */
  private static void build(Path repo, String sha, Path target) {
    Path source = null;
    try {
      source = Files.createTempDirectory("uc-old-source");
      Path archive = source.resolve("source.tar");
      git("-C", repo.toString(), "archive", "-o", archive.toString(), sha);
      run(new ProcessBuilder("tar", "-xf", archive.toString(), "-C", source.toString()));
      System.err.println("Building the server jar of " + sha + " in " + source);
      run(
          new ProcessBuilder("build/sbt", "-J-Xmx2G", "serverShaded/assembly")
              .directory(source.toFile())
              .redirectOutput(ProcessBuilder.Redirect.INHERIT));
      Path built;
      try (Stream<Path> jars = Files.list(source.resolve("server-shaded/target"))) {
        built =
            jars.filter(
                    p ->
                        p.getFileName()
                            .toString()
                            .startsWith("unitycatalog-server-shaded-assembly-"))
                .findFirst()
                .orElseThrow();
      }
      Files.createDirectories(target.getParent());
      Path partial = Files.createTempFile(target.getParent(), sha, ".partial");
      Files.copy(built, partial, StandardCopyOption.REPLACE_EXISTING);
      Files.move(partial, target, StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      delete(source);
    }
  }

  static String git(String... args) {
    List<String> command = new ArrayList<>(List.of("git"));
    command.addAll(List.of(args));
    return run(new ProcessBuilder(command)).trim();
  }

  /** Runs {@code builder}, returning its standard output; its errors go to this process's. */
  private static String run(ProcessBuilder builder) {
    try {
      Process process = builder.redirectError(ProcessBuilder.Redirect.INHERIT).start();
      String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      int exit = process.waitFor();
      if (exit != 0) {
        throw new IllegalStateException(
            String.join(" ", builder.command()) + " exited with " + exit);
      }
      return output;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static void delete(Path root) {
    if (root == null) {
      return;
    }
    try (Stream<Path> paths = Files.walk(root)) {
      paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
    } catch (IOException e) {
      // A leftover temp directory is harmless.
    }
  }
}
