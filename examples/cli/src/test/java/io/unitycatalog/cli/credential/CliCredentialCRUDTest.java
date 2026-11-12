package io.unitycatalog.cli.credential;

import static io.unitycatalog.cli.TestUtils.executeCliCommand;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.cli.CredentialCli;
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
import java.util.function.Supplier;
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
            "An Azure credential needs both --azure_directory_id and --azure_application_id.");
  }

  @Test
  public void testAzureClientSecretComesFromTheFlagThenTheEnvironmentThenAPrompt() {
    Map<String, String> env = Map.of("UC_AZURE_CLIENT_SECRET", "env-secret\n");
    Supplier<char[]> prompt = "typed-secret"::toCharArray;
    Supplier<char[]> noTerminal = () -> null;

    assertThat(CredentialCli.azureClientSecret("flag-secret", env, prompt))
        .isEqualTo("flag-secret");
    // A line break at the end, as in a secret read from a file, is dropped.
    assertThat(CredentialCli.azureClientSecret(null, env, prompt)).isEqualTo("env-secret");
    // An absent, empty or line-break-only variable falls back to the prompt.
    for (Map<String, String> unset :
        List.of(
            Map.<String, String>of(),
            Map.of("UC_AZURE_CLIENT_SECRET", ""),
            Map.of("UC_AZURE_CLIENT_SECRET", "\r\n"))) {
      assertThat(CredentialCli.azureClientSecret(null, unset, prompt)).isEqualTo("typed-secret");
    }
    assertThatThrownBy(() -> CredentialCli.azureClientSecret(null, Map.of(), noTerminal))
        .isInstanceOf(CliException.class)
        .hasMessage(
            "Set the Azure client secret in UC_AZURE_CLIENT_SECRET or with --azure_client_secret,"
                + " or run the command in a terminal to be prompted for it.");
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
