package io.unitycatalog.server.persist;

import io.unitycatalog.server.auth.decorator.KeyMapper;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.ServerProperties;
import lombok.Getter;
import org.hibernate.SessionFactory;

/**
 * Each server instance has a set of repositories that are used to interact with the database. This
 * class is used to create repositories once which are then shared across the server instance.
 */
@Getter
public class Repositories {
  private final SessionFactory sessionFactory;
  private final ExternalLocationUtils externalLocationUtils;
  private final StorageCredentialVendor storageCredentialVendor;
  private final FileOperations fileOperations;

  private final CatalogRepository catalogRepository;
  private final SchemaRepository schemaRepository;
  private final TableRepository tableRepository;
  private final StagingTableRepository stagingTableRepository;
  private final VolumeRepository volumeRepository;
  private final UserRepository userRepository;
  private final MetastoreRepository metastoreRepository;
  private final FunctionRepository functionRepository;
  private final ModelRepository modelRepository;
  private final CredentialRepository credentialRepository;
  private final ExternalLocationRepository externalLocationRepository;
  private final DeltaCommitRepository deltaCommitRepository;
  private final DependencyRepository dependencyRepository;

  private final KeyMapper keyMapper;

  public Repositories(SessionFactory sessionFactory, ServerProperties serverProperties) {
    this(sessionFactory, serverProperties, null);
  }

  /**
   * @param cloudCredentialVendor an injected cloud credential vendor (e.g. a test mock), or {@code
   *     null} to build the default from {@code serverProperties}. Owning the credential/file-IO
   *     chain here lets repositories read table storage (e.g. Delta commit files) without
   *     late-binding.
   */
  public Repositories(
      SessionFactory sessionFactory,
      ServerProperties serverProperties,
      CloudCredentialVendor cloudCredentialVendor) {
    this.sessionFactory = sessionFactory;
    this.externalLocationUtils = new ExternalLocationUtils(sessionFactory);
    CloudCredentialVendor resolvedCloudCredentialVendor =
        cloudCredentialVendor != null
            ? cloudCredentialVendor
            : new CloudCredentialVendor(serverProperties);
    this.storageCredentialVendor =
        new StorageCredentialVendor(resolvedCloudCredentialVendor, externalLocationUtils);
    this.fileOperations = new FileOperations(storageCredentialVendor, serverProperties);

    this.catalogRepository = new CatalogRepository(this, sessionFactory);
    this.schemaRepository = new SchemaRepository(this, sessionFactory);
    this.tableRepository = new TableRepository(this, sessionFactory, serverProperties);
    this.stagingTableRepository =
        new StagingTableRepository(this, sessionFactory, serverProperties);
    this.volumeRepository = new VolumeRepository(this, sessionFactory);
    this.userRepository = new UserRepository(this, sessionFactory);
    this.metastoreRepository = new MetastoreRepository(this, sessionFactory);
    this.functionRepository = new FunctionRepository(this, sessionFactory);
    this.modelRepository = new ModelRepository(this, sessionFactory, serverProperties);
    this.credentialRepository = new CredentialRepository(this, sessionFactory, serverProperties);
    this.externalLocationRepository = new ExternalLocationRepository(this, sessionFactory);
    this.deltaCommitRepository =
        new DeltaCommitRepository(sessionFactory, serverProperties, fileOperations);
    this.dependencyRepository = new DependencyRepository();

    // KeyMapper uses all the repositories above.
    this.keyMapper = new KeyMapper(this);
  }
}
