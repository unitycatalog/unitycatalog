package io.unitycatalog.server.persist;

import io.unitycatalog.server.auth.decorator.KeyMapper;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.FileOperationsImpl;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.service.credential.cache.StorageCredentialCache;
import io.unitycatalog.server.utils.ServerProperties;
import java.time.Clock;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
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
  private final StorageCleanupTaskRepository storageCleanupTaskRepository;

  private final KeyMapper keyMapper;

  public Repositories(SessionFactory sessionFactory, ServerProperties serverProperties) {
    this(sessionFactory, serverProperties, null);
  }

  public Repositories(
      SessionFactory sessionFactory,
      ServerProperties serverProperties,
      CloudCredentialVendor cloudCredentialVendor) {
    this(sessionFactory, serverProperties, cloudCredentialVendor, UnaryOperator.identity());
  }

  /**
   * @param cloudCredentialVendor an injected cloud credential vendor (e.g. a test mock), or {@code
   *     null} to build the default from {@code serverProperties}. Owning the credential/file-IO
   *     chain here lets repositories read table storage (e.g. Delta commit files) without
   *     late-binding.
   * @param fileOperationsDecorator wraps the default {@link FileOperations} before use ({@link
   *     UnaryOperator#identity()} leaves it unchanged); lets tests map cloud IO to local storage.
   */
  public Repositories(
      SessionFactory sessionFactory,
      ServerProperties serverProperties,
      CloudCredentialVendor cloudCredentialVendor,
      UnaryOperator<FileOperations> fileOperationsDecorator) {
    this.sessionFactory = sessionFactory;
    this.externalLocationUtils = new ExternalLocationUtils(sessionFactory);
    CloudCredentialVendor resolvedCloudCredentialVendor =
        cloudCredentialVendor != null
            ? cloudCredentialVendor
            : new CloudCredentialVendor(serverProperties);
    StorageCredentialCache credentialCache =
        serverProperties
            .getStorageCredentialCacheTestClockProvider()
            .map(
                fqcn ->
                    new StorageCredentialCache(
                        resolvedCloudCredentialVendor, serverProperties, loadTestClock(fqcn)))
            .orElseGet(
                () -> new StorageCredentialCache(resolvedCloudCredentialVendor, serverProperties));
    this.storageCredentialVendor =
        new StorageCredentialVendor(credentialCache, externalLocationUtils);
    this.fileOperations =
        fileOperationsDecorator.apply(
            new FileOperationsImpl(storageCredentialVendor, serverProperties));

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
    this.storageCleanupTaskRepository = new StorageCleanupTaskRepository(sessionFactory);

    // KeyMapper uses all the repositories above.
    this.keyMapper = new KeyMapper(this);
  }

  /**
   * Reflectively loads a test-only {@code Supplier<Clock>} (see {@link
   * ServerProperties#getStorageCredentialCacheTestClockProvider()}) and returns its clock. Mirrors
   * the reflective credential-generator hook. Only reached when the test seam property is set.
   */
  private static Clock loadTestClock(String fqcn) {
    try {
      Object provider = Class.forName(fqcn).getDeclaredConstructor().newInstance();
      @SuppressWarnings("unchecked")
      Supplier<Clock> clockSupplier = (Supplier<Clock>) provider;
      Clock clock = clockSupplier.get();
      if (clock == null) {
        throw new IllegalStateException(
            "storage-credential-cache test clock provider returned null: " + fqcn);
      }
      return clock;
    } catch (ReflectiveOperationException | ClassCastException e) {
      throw new IllegalStateException(
          "Failed to load storage-credential-cache test clock provider: " + fqcn, e);
    }
  }
}
