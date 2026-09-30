package io.unitycatalog.server.base.volume;

import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.BaseSchemaScopedTestEnv;
import org.junit.jupiter.api.BeforeEach;

/** Provides an auto-created catalog and schema plus initialized volume operations. */
public abstract class BaseVolumeCRUDTestEnv extends BaseSchemaScopedTestEnv {

  protected VolumeOperations volumeOperations;

  protected abstract VolumeOperations createVolumeOperations(ServerConfig serverConfig);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    volumeOperations = createVolumeOperations(serverConfig);
  }
}
