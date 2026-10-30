package io.unitycatalog.server.security;

import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.security.SecurityContext.Issuers;
import io.unitycatalog.server.utils.ServerProperties;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * Prints an admin token signed with the server's key, for creating the first users and grants:
 * {@code bin/uc --auth_token "$(bin/uc-admin-token)" user list}. It reads the key the server signs
 * with, from the token signing key file settings or {@code etc/conf}, and writes no key or token
 * files.
 */
public final class AdminTokenCommand {

  private AdminTokenCommand() {}

  public static void main(String[] args) throws Exception {
    // Logs go to the CLI log file, or nowhere when there is none (as in a Helm pod), so standard
    // output holds only the token.
    Path logConfig = Path.of("etc", "conf", "cli.log4j2.properties");
    if (Files.exists(logConfig)) {
      Configurator.initialize(null, logConfig.toString());
    } else {
      Configurator.setRootLevel(Level.OFF);
    }
    // Only reads the key: a missing key is an error here, not a reason to generate one.
    SecurityConfiguration securityConfiguration =
        SecurityConfiguration.loadExisting(
            new ServerProperties(UnityCatalogServer.SERVER_PROPERTIES_FILE),
            Path.of("etc", "conf"));
    System.out.println(
        SecurityContext.createServiceToken(
            securityConfiguration.algorithmRSA(), "server", Issuers.INTERNAL));
  }
}
