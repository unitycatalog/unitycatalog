package io.unitycatalog.server.migrate.kit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * An old server an {@link UpgradePath} runs: a published release image, or the shaded server jar
 * built from a git ref. Resolved on first use, so listing the paths builds nothing.
 */
public final class OldImage {

  /** A prebuilt base jar (CI restores main's from its cache), used instead of building one. */
  public static final String BASE_JAR_ENV = "UC_TEST_BASE_JAR";

  // The published release images on Docker Hub, pinned by digest: one constant per release.
  public static final OldImage V0_6_0 =
      published(
          "v0.6.0",
          "unitycatalog/unitycatalog@sha256:606349a178d99c3b42d9c9c8cfea72f04efb3cfc350ee89eec45368b96e50086");
  public static final OldImage V0_7_0 =
      published(
          "v0.7.0",
          "unitycatalog/unitycatalog@sha256:cdacdc5e7624795e7b2a18ece0d7357c5a1ec640db17fccb4d0fb208dd0d335b");

  private static final Path JAR_CACHE =
      Path.of(System.getProperty("user.home"), ".cache", "uc-upgrade-base");

  private final String description;
  private final boolean jar;
  private final Supplier<String> resolver;
  private String name;

  private OldImage(String description, boolean jar, Supplier<String> resolver) {
    this.description = description;
    this.jar = jar;
    this.resolver = resolver;
  }

  /** An image from a registry, pinned by digest; {@code tag} names it in logs. */
  private static OldImage published(String tag, String name) {
    return new OldImage(tag, false, () -> name);
  }

  /**
   * The shaded server jar of {@code ref}: {@value #BASE_JAR_ENV} if set, else the one cached for
   * the ref's commit in {@code ~/.cache/uc-upgrade-base}, else built once and cached.
   */
  public static OldImage jarBuiltFrom(String ref) {
    return new OldImage("jar built from " + ref, true, () -> jar(ref));
  }

  /** Whether this is a server jar, run as a process, rather than an image. */
  public boolean isJar() {
    return jar;
  }

  /** The image name or the jar's path, building the jar first if needed. */
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

  private static String jar(String ref) {
    String prebuilt = System.getenv(BASE_JAR_ENV);
    if (prebuilt != null && !prebuilt.isBlank()) {
      return prebuilt;
    }
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
      source = Files.createTempDirectory("uc-base-source");
      Path archive = source.resolve("source.tar");
      git("-C", repo.toString(), "archive", "-o", archive.toString(), sha);
      run(new ProcessBuilder("tar", "-xf", archive.toString(), "-C", source.toString()));
      if (System.getenv("MAVEN_PROXY_URL") != null) {
        // build/sbt fetches its launcher from the mirror's root, which a Maven mirror may not
        // serve. Reuse the launcher this checkout already downloaded (git-ignored, so not in the
        // archive).
        try (DirectoryStream<Path> launchers =
            Files.newDirectoryStream(repo.resolve("build"), "sbt-launch-*.jar")) {
          for (Path launcher : launchers) {
            Files.copy(launcher, source.resolve("build").resolve(launcher.getFileName()));
          }
        }
      }
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

  private static String git(String... args) {
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
