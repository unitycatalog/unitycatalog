package io.unitycatalog.server.persist;

import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.hibernate.SessionFactory;

public class RepositoryFactory {
  private final Map<Class<?>, Object> repositories = new HashMap<>();
  @Getter private final SessionFactory sessionFactory;
  @Getter private final FileOperations fileOperations;

  public RepositoryFactory(SessionFactory sessionFactory, ServerProperties serverProperties) {
    this.sessionFactory = sessionFactory;
    this.fileOperations = new FileOperations(serverProperties);
    initializeRepositories();
  }

  public void initializeRepositories() {
    repositories.put(CatalogRepository.class, new CatalogRepository(this, sessionFactory));
    repositories.put(SchemaRepository.class, new SchemaRepository(this, sessionFactory));
    repositories.put(TableRepository.class, new TableRepository(this, sessionFactory));
    repositories.put(VolumeRepository.class, new VolumeRepository(this, sessionFactory));
    repositories.put(UserRepository.class, new UserRepository(this, sessionFactory));
    repositories.put(MetastoreRepository.class, new MetastoreRepository(this, sessionFactory));
    repositories.put(FunctionRepository.class, new FunctionRepository(this, sessionFactory));
    repositories.put(ModelRepository.class, new ModelRepository(this, sessionFactory));
  }

  @SuppressWarnings("unchecked")
  public <T> T getRepository(Class<T> repositoryClass) {
    return (T) repositories.get(repositoryClass);
  }
}
