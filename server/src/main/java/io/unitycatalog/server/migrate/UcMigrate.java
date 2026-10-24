package io.unitycatalog.server.migrate;

import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.migrate.SchemaMigrator.MigrationOutcome;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.PrintStream;
import java.util.List;
import java.util.Properties;
import org.flywaydb.core.api.MigrationInfo;

/**
 * {@code bin/uc-migrate}: sets up and upgrades the schema of the database in {@code
 * etc/conf/hibernate.properties}.
 *
 * <ul>
 *   <li>{@code migrate}: sets up an empty database, adopts a 0.7 one, applies the pending steps.
 *   <li>{@code info}: shows the state, the current and latest version, and the applied and pending
 *       steps.
 * </ul>
 */
public final class UcMigrate {

  private static final String USAGE = "Usage: bin/uc-migrate migrate | info";

  private UcMigrate() {}

  public static void main(String[] args) {
    ServerProperties serverProperties =
        new ServerProperties(UnityCatalogServer.SERVER_PROPERTIES_FILE);
    System.exit(
        run(
            args,
            HibernateConfigurator.setupHibernateProperties(serverProperties),
            System.out,
            System.err));
  }

  /** Returns the exit code: 0 on success, 1 on failure (the cause is printed to {@code err}). */
  static int run(String[] args, Properties hibernateProperties, PrintStream out, PrintStream err) {
    if (args.length != 1 || !List.of("migrate", "info").contains(args[0])) {
      err.println(USAGE);
      return 1;
    }
    try (SchemaMigrator migrator = SchemaMigrator.fromHibernateProperties(hibernateProperties)) {
      if (args[0].equals("migrate")) {
        MigrationOutcome outcome = migrator.migrate();
        out.printf(
            "Schema version %s -> %s (%d %s)%n",
            orNone(outcome.from()),
            outcome.to(),
            outcome.applied(),
            outcome.applied() == 1 ? "step" : "steps");
      } else {
        out.println("State: " + migrator.state());
        out.println("Current version: " + orNone(migrator.currentVersion()));
        out.println("Latest version: " + migrator.latestVersion());
        printSteps(out, "Applied steps", migrator.applied());
        printSteps(out, "Pending steps", migrator.pending());
      }
      return 0;
    } catch (RuntimeException e) {
      err.println(e.getMessage() != null ? e.getMessage() : e.toString());
      return 1;
    }
  }

  private static void printSteps(PrintStream out, String title, List<MigrationInfo> steps) {
    out.println(title + ":" + (steps.isEmpty() ? " none" : ""));
    for (MigrationInfo step : steps) {
      out.println("  " + step.getVersion() + "  " + step.getDescription());
    }
  }

  private static String orNone(String version) {
    return version == null ? "none" : version;
  }
}
