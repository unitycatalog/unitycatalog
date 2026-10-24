package io.unitycatalog.server.migrate.kit;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.ParameterDeclarations;

/**
 * The ways a database reaches this code: the old servers that write it, run in order on one
 * database. This enum is the only list; adding a path is one constant. {@value #ENV} (a comma list
 * of names; unset means all) runs a subset.
 */
public enum UpgradePath {
  /** The server built from the code this change starts from ({@value #BASE_REF_ENV}). */
  BASE(OldImage.builtFrom(baseRef())),
  /** The newest release on Docker Hub, so each new release is tested with no code change. */
  LATEST(OldImage.published("unitycatalog/unitycatalog:latest")),
  /** 0.6 writes the data, then 0.7 starts on it and applies its schema changes (auto-update). */
  V0_6_VIA_0_7(
      OldImage.published("unitycatalog/unitycatalog:v0.6.0"),
      OldImage.published("unitycatalog/unitycatalog:v0.7.0"));

  public static final String ENV = "UC_TEST_UPGRADE_PATHS";
  public static final String BASE_REF_ENV = "UC_TEST_BASE_REF";

  public final List<OldImage> images;

  UpgradePath(OldImage... images) {
    this.images = List.of(images);
  }

  /** The git ref {@link #BASE} is built from: {@value #BASE_REF_ENV}, unset or empty means main. */
  private static String baseRef() {
    String ref = System.getenv(BASE_REF_ENV);
    return ref == null || ref.isBlank() ? "origin/main" : ref;
  }

  /** The paths {@value #ENV} selects. */
  public static List<UpgradePath> selected() {
    String value = System.getenv(ENV);
    if (value == null || value.isBlank()) {
      return List.of(values());
    }
    Set<String> names =
        Arrays.stream(value.split(",")).map(String::trim).collect(Collectors.toSet());
    List<UpgradePath> paths =
        Arrays.stream(values()).filter(path -> names.remove(path.name())).toList();
    if (!names.isEmpty()) {
      throw new IllegalArgumentException(
          ENV + "=" + value + " names " + names + ", not in " + Arrays.toString(values()));
    }
    return paths;
  }

  /** Supplies {@link #selected()} to {@link AcrossUpgradePaths}. */
  public static final class Provider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(
        ParameterDeclarations parameters, ExtensionContext context) {
      return selected().stream().map(Arguments::of);
    }
  }
}
