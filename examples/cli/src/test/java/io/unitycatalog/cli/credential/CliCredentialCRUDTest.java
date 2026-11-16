package io.unitycatalog.cli.credential;

import static io.unitycatalog.cli.TestUtils.executeCliCommand;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.cli.catalog.CliCatalogOperations;
import io.unitycatalog.cli.externallocation.CliExternalLocationOperations;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.credential.BaseCredentialCRUDTest;
import io.unitycatalog.server.base.credential.CredentialOperations;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class CliCredentialCRUDTest extends BaseCredentialCRUDTest {

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

  @Test
  public void testAzureFlagsMustBeGivenTogether() {
    assertThatThrownBy(
            () ->
                executeCliCommand(
                    serverConfig,
                    List.of(
                        "credential",
                        "create",
                        "--name",
                        "azure_cred",
                        "--azure_directory_id",
                        "directory-id",
                        "--azure_client_secret",
                        "secret")))
        .isInstanceOf(CliException.class)
        .hasMessage(
            "--azure_directory_id, --azure_application_id and --azure_client_secret must be set"
                + " together.");
  }

  @Test
  public void testKeyFileMustBeAServiceAccountKeyFile(@TempDir Path dir) throws Exception {
    Path keyFile = dir.resolve("key.json");
    // A key file without its private_key_id.
    new ObjectMapper()
        .writeValue(
            keyFile.toFile(),
            Map.of(
                "client_email",
                "uc@project.iam.gserviceaccount.com",
                "private_key",
                TestUtils.generatePkcs8PrivateKeyPem()));
    assertThatThrownBy(() -> createWithKeyFile(keyFile))
        .isInstanceOf(CliException.class)
        .hasMessage(keyFile + " is not a service account key file: it has no private_key_id.");

    Path missing = dir.resolve("missing.json");
    assertThatThrownBy(() -> createWithKeyFile(missing))
        .isInstanceOf(CliException.class)
        .hasMessage("Cannot read the service account key file " + missing);
    assertThat(credentialOperations.listCredentials(Optional.empty(), CredentialPurpose.STORAGE))
        .isEmpty();
  }

  private void createWithKeyFile(Path keyFile) {
    executeCliCommand(
        serverConfig,
        List.of(
            "credential",
            "create",
            "--name",
            "gcs_cred",
            "--gcp_service_account_key_file",
            keyFile.toString()));
  }
}
