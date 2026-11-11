package io.unitycatalog.server.security;

import com.auth0.jwt.algorithms.Algorithm;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.security.SecurityContext.Issuers;
import io.unitycatalog.server.utils.ServerProperties;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * Prints an admin token signed with the server's key, for creating the first users and grants:
 * {@code bin/uc --auth_token "$(bin/uc-admin-token)" user list}. It reads the key the server signs
 * with, from the token signing key file setting or {@code etc/conf}, and writes no key or token
 * files. Run it with the server's environment, which can hold that setting.
 *
 * <p>With {@code --scim-principal <email> --idp <name> --lifetime <ISO-8601 duration>}, it prints
 * an IdP's SCIM token instead, accepted only on the SCIM API. It opens the server's database, as
 * {@code bin/start-uc-server --upgrade-schema} does, and marks that user as the IdP's principal,
 * which can then no longer sign in. Disabling the user revokes the token.
 */
public final class AdminTokenCommand {

  private static final Path CONFIGURATION_FOLDER = Path.of("etc", "conf");
  private static final String USAGE =
      "Usage: bin/uc-admin-token [--scim-principal <email> --idp <name> --lifetime <ISO-8601"
          + " duration>]";

  private AdminTokenCommand() {}

  public static void main(String[] args) throws Exception {
    // Logs go to the CLI log file, or nowhere when there is none (as in a Helm pod), so standard
    // output holds only the token.
    Path logConfig = CONFIGURATION_FOLDER.resolve("cli.log4j2.properties");
    if (Files.exists(logConfig)) {
      Configurator.initialize(null, logConfig.toString());
    } else {
      Configurator.setRootLevel(Level.OFF);
    }
    ServerProperties serverProperties =
        new ServerProperties(UnityCatalogServer.SERVER_PROPERTIES_FILE);
    // Only reads the key: a missing key is an error here, not a reason to generate one.
    Algorithm algorithm =
        SecurityConfiguration.loadExisting(serverProperties, CONFIGURATION_FOLDER).algorithmRSA();
    if (args.length == 0) {
      System.out.println(SecurityContext.createServiceToken(algorithm, "server", Issuers.INTERNAL));
      return;
    }
    try {
      // Checks the options before it opens the database.
      ScimTokenOptions options = ScimTokenOptions.parse(args);
      Path hibernateFile = CONFIGURATION_FOLDER.resolve("hibernate.properties");
      if (!Files.isRegularFile(hibernateFile)) {
        throw new IllegalArgumentException(
            "The configuration file "
                + hibernateFile.toAbsolutePath()
                + " is not found: run bin/uc-admin-token in the Unity Catalog folder, the one that"
                + " holds etc/conf.");
      }
      try (HibernateConfigurator database =
          new HibernateConfigurator(
              HibernateConfigurator.setupHibernateProperties(serverProperties, hibernateFile))) {
        System.out.println(
            createScimToken(
                options,
                algorithm,
                new UserRepository(/* repositories= */ null, database.getSessionFactory())));
      }
    } catch (IllegalArgumentException | BaseException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
  }

  /**
   * Marks the user that {@code --scim-principal} names as the principal of the IdP that {@code
   * --idp} names, and returns its SCIM token, which expires after {@code --lifetime}.
   *
   * @throws IllegalArgumentException for options it can't mint a token from
   * @throws BaseException when the user is missing, is the admin user, or is already the principal
   *     of another IdP
   */
  public static String createScimToken(String[] args, Algorithm algorithm, UserRepository users) {
    return createScimToken(ScimTokenOptions.parse(args), algorithm, users);
  }

  private static String createScimToken(
      ScimTokenOptions options, Algorithm algorithm, UserRepository users) {
    User principal = users.setIdpName(options.email(), options.idpName());
    return SecurityContext.createScimToken(
        algorithm,
        "server",
        Issuers.INTERNAL,
        principal.getId(),
        options.idpName(),
        options.lifetime());
  }

  private record ScimTokenOptions(String email, String idpName, Duration lifetime) {

    static ScimTokenOptions parse(String[] args) {
      String email = null;
      String idpName = null;
      String lifetime = null;
      for (int i = 0; i < args.length; i += 2) {
        if (i + 1 == args.length || args[i + 1].isBlank()) {
          throw new IllegalArgumentException(args[i] + " needs a value. " + USAGE);
        }
        switch (args[i]) {
          case "--scim-principal" -> email = args[i + 1];
          case "--idp" -> idpName = args[i + 1];
          case "--lifetime" -> lifetime = args[i + 1];
          default ->
              throw new IllegalArgumentException("Unknown option: " + args[i] + ". " + USAGE);
        }
      }
      if (email == null) {
        throw new IllegalArgumentException(
            "--scim-principal is required with --idp and --lifetime. " + USAGE);
      }
      if (idpName == null) {
        throw new IllegalArgumentException("--idp is required with --scim-principal. " + USAGE);
      }
      if (lifetime == null) {
        throw new IllegalArgumentException(
            "--lifetime is required with --scim-principal. " + USAGE);
      }
      return new ScimTokenOptions(email, idpName, parseLifetime(lifetime));
    }

    private static Duration parseLifetime(String lifetime) {
      Duration duration;
      try {
        duration = Duration.parse(lifetime);
      } catch (DateTimeParseException e) {
        duration = null;
      }
      if (duration == null || duration.isNegative() || duration.isZero()) {
        throw new IllegalArgumentException(
            "--lifetime must be a positive ISO-8601 duration, such as P90D: " + lifetime);
      }
      return duration;
    }
  }
}
