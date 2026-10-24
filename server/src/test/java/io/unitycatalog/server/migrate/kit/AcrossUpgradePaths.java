package io.unitycatalog.server.migrate.kit;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Runs a test once per {@link UpgradePath} on the selected test database. Tagged {@code upgrades}:
 * it runs released server images in Docker, so it stays out of the default test run ({@code
 * build/sbt testUpgrades} runs it).
 */
@Target(METHOD)
@Retention(RUNTIME)
@Tag("upgrades")
@ParameterizedTest(name = "{0}")
@ArgumentsSource(UpgradePath.Provider.class)
public @interface AcrossUpgradePaths {}
