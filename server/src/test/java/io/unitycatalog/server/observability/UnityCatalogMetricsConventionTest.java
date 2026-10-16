package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Checks the UC domain metrics against the rules in {@link UnityCatalogMetrics}. */
public class UnityCatalogMetricsConventionTest {
  // Lowercase words separated by dots, starting with "uc".
  private static final Pattern METER_NAME = Pattern.compile("uc(\\.[a-z][a-z0-9_]*)+");

  private SimpleMeterRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    new UnityCatalogMetrics(registry);
  }

  @AfterEach
  void tearDown() {
    registry.close();
  }

  @Test
  void everyCatalogMetricNameStartsWithUc() {
    assertThat(UcMetric.values())
        .isNotEmpty()
        .allSatisfy(metric -> assertThat(metric.getMetricName()).matches(METER_NAME));
  }

  @Test
  void everyCatalogMetricHasADescription() {
    assertThat(UcMetric.values())
        .allSatisfy(metric -> assertThat(metric.getDescription()).as("%s", metric).isNotBlank());
  }

  /**
   * The registered UC meters are exactly the {@link UcMetric} entries, so a meter registered under
   * a name outside the catalog fails, as does a catalog entry that no area registers.
   */
  @Test
  void registeredMetersAreExactlyTheCatalog() {
    Set<String> registered =
        meters().stream().map(meter -> meter.getId().getName()).collect(Collectors.toSet());

    assertThat(registered)
        .isEqualTo(
            Arrays.stream(UcMetric.values())
                .map(UcMetric::getMetricName)
                .collect(Collectors.toSet()));
  }

  /**
   * An area whose instance fields are only meters and primitives holds no registry, directly or
   * through a {@code MeterProvider} or lambda, so it can register meters only in its constructor.
   * One whose methods take only primitives, enums, and durations can tag them only with values
   * fixed in its code. Together these keep every series known at startup, where the other checks
   * see it.
   */
  @Test
  void areasRegisterAllMetersAtStartupWithFixedTagValues() {
    List<Class<?>> areas =
        Arrays.stream(UnityCatalogMetrics.class.getDeclaredFields())
            .filter(field -> !Modifier.isStatic(field.getModifiers()))
            .<Class<?>>map(Field::getType)
            .toList();

    assertThat(areas)
        .isNotEmpty()
        .allSatisfy(
            area -> {
              assertThat(instanceFields(area))
                  .as("instance fields of %s", area.getSimpleName())
                  .allMatch(
                      field ->
                          field.getType().isPrimitive()
                              || Meter.class.isAssignableFrom(field.getType()));
              assertThat(publicMethods(area))
                  .allSatisfy(
                      method ->
                          assertThat(method.getParameterTypes())
                              .as("parameters of %s.%s", area.getSimpleName(), method.getName())
                              .allMatch(
                                  type ->
                                      type.isPrimitive()
                                          || type.isEnum()
                                          || type == Duration.class));
            });
  }

  private List<Meter> meters() {
    return registry.getMeters();
  }

  private static List<Field> instanceFields(Class<?> type) {
    return Arrays.stream(type.getDeclaredFields())
        .filter(field -> !Modifier.isStatic(field.getModifiers()) && !field.isSynthetic())
        .toList();
  }

  private static List<Method> publicMethods(Class<?> type) {
    return Arrays.stream(type.getDeclaredMethods())
        .filter(method -> Modifier.isPublic(method.getModifiers()))
        .toList();
  }
}
