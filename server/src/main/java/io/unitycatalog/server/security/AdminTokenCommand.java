package io.unitycatalog.server.security;

import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.security.SecurityContext.Issuers;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * Prints an admin token signed with the server's key, for creating the first users and grants:
 * {@code bin/uc --auth_token "$(bin/uc-admin-token)" user list}. It reads the key the server signs
 * with, from the token signing key file setting or {@code etc/conf}, and writes no key or token
 * files. Run it with the server's environment, which can hold that setting.
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
    try {
      run(
          new ServerProperties(UnityCatalogServer.SERVER_PROPERTIES_FILE),
          Path.of("etc", "conf"),
          System.out);
    } catch (Exception e) {
      // One line, not a stack trace: a missing key, or one this user cannot read.
      System.err.println(
          "uc-admin-token: " + (e instanceof IllegalStateException ? e.getMessage() : e));
      System.exit(1);
    }
  }

  // Prints one admin token, signed with the key the server uses, to out.
  static void run(ServerProperties serverProperties, Path configurationFolder, PrintStream out) {
    // Only reads the key: a missing key is an error here, not a reason to generate one.
    SecurityConfiguration securityConfiguration =
        SecurityConfiguration.loadExisting(serverProperties, configurationFolder);
    out.println(
        SecurityContext.createServiceToken(
            securityConfiguration.algorithmRSA(), "server", Issuers.INTERNAL));
  }
}
