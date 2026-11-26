package io.unitycatalog.cli.credential;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.cli.catalog.CliCatalogOperations;
import io.unitycatalog.cli.externallocation.CliExternalLocationOperations;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.credential.BaseCredentialCRUDTest;
import io.unitycatalog.server.base.credential.CredentialOperations;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

public class CliCredentialCRUDTest extends BaseCredentialCRUDTest {

  @Test
  public void testUnreadableKeyFileIsReported() {
    assertThatThrownBy(
            () ->
                ((CliCredentialOperations) credentialOperations)
                    .execute(
                        CredentialInfo.class,
                        "create",
                        List.of(
                            "--name",
                            "uc_testcredential",
                            "--gcp_service_account_key_file",
                            "/nonexistent/key.json")))
        .isInstanceOf(CliException.class)
        .hasMessageContaining("Cannot read the key file /nonexistent/key.json");
  }

  @Test
  public void testMalformedKeyFileIsNotEchoed() throws Exception {
    Path file = Files.createTempFile("uc-bad-key", ".json");
    try {
      Files.writeString(file, "{\"private_key\": SECRET_MARKER_123");
      assertThatThrownBy(
              () ->
                  ((CliCredentialOperations) credentialOperations)
                      .execute(
                          CredentialInfo.class,
                          "create",
                          List.of(
                              "--name",
                              "uc_testcredential",
                              "--gcp_service_account_key_file",
                              file.toString())))
          .isInstanceOf(CliException.class)
          .hasMessageContaining("is not valid JSON")
          .hasMessageNotContaining("SECRET_MARKER_123");
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Override
  protected CredentialOperations createCredentialOperations(ServerConfig config) {
    return new CliCredentialOperations(config);
  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new CliCatalogOperations(config);
  }

  @Override
  protected ExternalLocationOperations createExternalLocationOperations(ServerConfig config) {
    return new CliExternalLocationOperations(config);
  }
}
