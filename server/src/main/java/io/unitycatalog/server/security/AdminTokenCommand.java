package io.unitycatalog.server.security;

import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.security.SecurityContext.Issuers;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.nio.file.Path;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * Prints an admin token signed with the server's key, for creating the first users and grants:
 * {@code bin/uc --auth_token $(bin/uc-admin-token) user list}. It reads the key the server signs
 * with, from {@code server.token-signing-key-dir} or {@code etc/conf}, and writes no files.
 */
public final class AdminTokenCommand {

  private AdminTokenCommand() {}

  public static void main(String[] args) throws Exception {
    // Logs go to the CLI log file, so standard output holds only the token.
    Configurator.initialize(null, "etc/conf/cli.log4j2.properties");
    String keyDirectory =
        new ServerProperties(UnityCatalogServer.SERVER_PROPERTIES_FILE)
            .get(Property.TOKEN_SIGNING_KEY_DIR);
    // Only reads the key: a missing key is an error here, not a reason to generate one.
    SecurityConfiguration securityConfiguration =
        SecurityConfiguration.fromKeyDirectory(
            Path.of(keyDirectory != null ? keyDirectory : "etc/conf"));
    System.out.println(
        SecurityContext.createServiceToken(
            securityConfiguration.algorithmRSA(),
            securityConfiguration.getKeyId(),
            "server",
            Issuers.INTERNAL));
  }
}
