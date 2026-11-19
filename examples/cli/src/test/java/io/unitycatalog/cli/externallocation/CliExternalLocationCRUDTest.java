package io.unitycatalog.cli.externallocation;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.cli.TestUtils;
import io.unitycatalog.cli.catalog.CliCatalogOperations;
import io.unitycatalog.cli.credential.CliCredentialOperations;
import io.unitycatalog.cli.schema.CliSchemaOperations;
import io.unitycatalog.cli.table.CliTableOperations;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.cli.volume.CliVolumeOperations;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.credential.CredentialOperations;
import io.unitycatalog.server.base.externallocation.BaseExternalLocationCRUDTest;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.base.volume.VolumeOperations;
import java.util.List;
import org.junit.jupiter.api.Test;

public class CliExternalLocationCRUDTest extends BaseExternalLocationCRUDTest {

  @Override
  protected ExternalLocationOperations createExternalLocationOperations(ServerConfig config) {
    return new CliExternalLocationOperations(config);
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new CliSchemaOperations(config);
  }

  @Override
  protected VolumeOperations createVolumeOperations(ServerConfig config) {
    return new CliVolumeOperations(config);
  }

  @Override
  protected TableOperations createTableOperations(ServerConfig config) {
    return new CliTableOperations(config);
  }

  @Override
  protected CredentialOperations createCredentialOperations(ServerConfig config) {
    return new CliCredentialOperations(config);
  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new CliCatalogOperations(config);
  }

  @Test
  public void testForceAloneIsNotAnUpdate() {
    assertThatThrownBy(
            () ->
                TestUtils.executeCliCommand(
                    serverConfig,
                    List.of("external_location", "update", "--name", "any", "--force", "true")))
        .isInstanceOf(CliException.class)
        .hasMessageStartingWith("No parameters to update")
        .hasMessageContaining("--url")
        .hasMessageNotContaining("--force");
  }
}
