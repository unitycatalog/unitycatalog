package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.FileOperationsImpl;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.service.credential.CachingCloudCredentialVendor;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.CooperativeDeadline;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RepositoriesTest {

  private SessionFactory sessionFactory;
  private ServerProperties serverProperties;

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    serverProperties = new ServerProperties(properties);
    Properties hibernateProperties =
        HibernateConfigurator.setupHibernateProperties(serverProperties);
    hibernateProperties.setProperty(
        "hibernate.connection.url", "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    sessionFactory = new HibernateConfigurator(hibernateProperties).getSessionFactory();
  }

  @AfterEach
  void tearDown() {
    sessionFactory.close();
  }

  @Test
  void decoratorWrapsTheFileOperationsRepositoriesUse() {
    AtomicReference<FileOperations> decorated = new AtomicReference<>();
    UnaryOperator<FileOperations> decorator =
        delegate -> {
          FileOperations wrapper = new DelegatingFileOperations(delegate);
          decorated.set(wrapper);
          return wrapper;
        };

    Repositories repositories =
        new Repositories(
            sessionFactory, serverProperties, /* cloudCredentialVendor= */ null, decorator);

    // The decorator ran on the server-built default and its result is the instance repositories
    // expose (and hand to DeltaCommitRepository), so the seam is actually wired end to end.
    assertThat(decorated.get()).isInstanceOf(DelegatingFileOperations.class);
    assertThat(repositories.getFileOperations()).isSameAs(decorated.get());
    assertThat(((DelegatingFileOperations) decorated.get()).delegate)
        .isInstanceOf(FileOperationsImpl.class);
  }

  @Test
  void identityDecoratorLeavesTheDefaultFileOperationsInPlace() {
    Repositories repositories =
        new Repositories(
            sessionFactory,
            serverProperties,
            /* cloudCredentialVendor= */ null,
            UnaryOperator.identity());

    assertThat(repositories.getFileOperations()).isInstanceOf(FileOperationsImpl.class);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void defaultCloudCredentialVendorCachesOnlyWhenTheCacheIsEnabled(boolean cacheEnabled) {
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    properties.setProperty("server.storage-credential-cache.enabled", String.valueOf(cacheEnabled));

    Repositories repositories = new Repositories(sessionFactory, new ServerProperties(properties));

    assertThat(repositories.getCloudCredentialVendor().getClass())
        .isEqualTo(cacheEnabled ? CachingCloudCredentialVendor.class : CloudCredentialVendor.class);
  }

  @Test
  void injectedCloudCredentialVendorIsUsedAsIs() {
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    properties.setProperty("server.storage-credential-cache.enabled", "true");
    CloudCredentialVendor injected = mock(CloudCredentialVendor.class);

    Repositories repositories =
        new Repositories(sessionFactory, new ServerProperties(properties), injected);

    assertThat(repositories.getCloudCredentialVendor()).isSameAs(injected);
  }

  /** Test wrapper that records its delegate; methods forward, but the tests only check identity. */
  private static final class DelegatingFileOperations implements FileOperations {
    private final FileOperations delegate;

    DelegatingFileOperations(FileOperations delegate) {
      this.delegate = delegate;
    }

    @Override
    public FileIO getFileIO(NormalizedURL path, Set<CredentialContext.Privilege> privileges) {
      return delegate.getFileIO(path, privileges);
    }

    @Override
    public SupportsPrefixOperations getCleanupFileIO(
        NormalizedURL path, CooperativeDeadline deadline) {
      return delegate.getCleanupFileIO(path, deadline);
    }

    @Override
    public Map<String, String> getFileIOConfig(
        NormalizedURL path,
        Set<CredentialContext.Privilege> privileges,
        Optional<String> credentialsEndpoint) {
      return delegate.getFileIOConfig(path, privileges, credentialsEndpoint);
    }
  }
}
