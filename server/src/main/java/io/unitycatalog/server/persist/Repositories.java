package io.unitycatalog.server.persist;

import io.unitycatalog.server.persist.utils.FileOperations;
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
  private final FileOperations fileOperations;

  private final CatalogRepository catalogRepository;
  private final SchemaRepository schemaRepository;
  private final TableRepository tableRepository;
  private final VolumeRepository volumeRepository;
  private final UserRepository userRepository;
  private final MetastoreRepository metastoreRepository;
  private final FunctionRepository functionRepository;
  private final ModelRepository modelRepository;

  public Repositories(SessionFactory sessionFactory, ServerProperties serverProperties) {
    this.sessionFactory = sessionFactory;
    this.fileOperations = new FileOperations(serverProperties);

    this.catalogRepository = new CatalogRepository(this, sessionFactory);
    this.schemaRepository = new SchemaRepository(this, sessionFactory);
    this.tableRepository = new TableRepository(this, sessionFactory);
    this.volumeRepository = new VolumeRepository(this, sessionFactory);
    this.userRepository = new UserRepository(this, sessionFactory);
    this.metastoreRepository = new MetastoreRepository(this, sessionFactory);
    this.functionRepository = new FunctionRepository(this, sessionFactory);
    this.modelRepository = new ModelRepository(this, sessionFactory);
  }
}
