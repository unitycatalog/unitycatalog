package io.unitycatalog.server.security;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;
import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SecurityContextTest {

  @TempDir Path configurationFolder;

  @Test
  void keepsTheAdminTokenInMemoryAndDeletesAStaleTokenFile() throws Exception {
    Files.writeString(configurationFolder.resolve("token.txt"), "token from an earlier version");

    SecurityConfiguration securityConfiguration = new SecurityConfiguration(configurationFolder);
    SecurityContext securityContext =
        new SecurityContext(configurationFolder, securityConfiguration, "server", INTERNAL);

    DecodedJWT token =
        JWT.require(securityConfiguration.algorithmRSA())
            .build()
            .verify(securityContext.getServiceToken());
    assertThat(token.getClaim(JwtClaim.SUBJECT.key()).asString()).isEqualTo("admin");
    assertThat(configurationFolder.resolve("token.txt")).doesNotExist();
  }
}
