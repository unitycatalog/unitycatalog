package io.unitycatalog.server.base.volume;

import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.BaseSchemaCRUDTestEnv;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;

/**
 * Fixture for volume tests: the catalog+schema from {@link BaseSchemaCRUDTestEnv} plus volume ops.
 * The catalog is created with a storage root because a managed volume derives its location from the
 * catalog/schema managed storage, unlike managed tables which fall back to {@code
 * TABLE_STORAGE_ROOT}. Carries no {@code @Test} methods so subclasses can extend it freely.
 */
public abstract class BaseVolumeCRUDTestEnv extends BaseSchemaCRUDTestEnv {

  protected VolumeOperations volumeOperations;

  protected abstract VolumeOperations createVolumeOperations(ServerConfig serverConfig);

  @Override
  protected Optional<String> catalogStorageRoot() {
    return Optional.of(NormalizedURL.normalize(testDirectoryRoot.toString()));
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    volumeOperations = createVolumeOperations(serverConfig);
  }
}
