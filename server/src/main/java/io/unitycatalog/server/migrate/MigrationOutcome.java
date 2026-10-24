package io.unitycatalog.server.migrate;

/**
 * The result of {@link Migrator#migrate()}.
 *
 * @param from the schema version before the run, {@code null} for an empty database
 * @param to the schema version after the run
 * @param applied the number of steps the run applied
 */
public record MigrationOutcome(String from, String to, int applied) {}
