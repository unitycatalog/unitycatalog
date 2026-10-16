package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.UnityAccessUtil;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.CreateUser;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.model.UpdateUser;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Properties;
import java.util.UUID;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class UserRepositoryTest {

  private static SessionFactory sessionFactory;
  private static UserRepository userRepository;
  private static HibernateConfigurator hibernateConfigurator;

  @BeforeAll
  static void setUp() {
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    hibernateConfigurator = new HibernateConfigurator(new ServerProperties(properties));
    sessionFactory = hibernateConfigurator.getSessionFactory();
    userRepository = new UserRepository(null, sessionFactory);
  }

  @AfterAll
  static void tearDown() {
    if (hibernateConfigurator != null) {
      hibernateConfigurator.close();
    }
  }

  @Test
  void createUserRejectsDuplicateExternalId() {
    userRepository.createUser(
        CreateUser.builder()
            .name("Service A")
            .email("service-a@example.com")
            .externalId("oauth-client-a")
            .build());

    assertThatThrownBy(
            () ->
                userRepository.createUser(
                    CreateUser.builder()
                        .name("Service B")
                        .email("service-b@example.com")
                        .externalId("oauth-client-a")
                        .build()))
        .isInstanceOf(BaseException.class)
        .satisfies(
            e ->
                assertThat(((BaseException) e).getErrorCode()).isEqualTo(ErrorCode.ALREADY_EXISTS));
  }

  @Test
  void createUserAllowsMultipleNullExternalIds() {
    userRepository.createUser(
        CreateUser.builder().name("Human A").email("human-a@example.com").build());
    userRepository.createUser(
        CreateUser.builder().name("Human B").email("human-b@example.com").build());
  }

  @Test
  void updateUserRejectsDuplicateExternalIdOnAnotherUser() {
    var first =
        userRepository.createUser(
            CreateUser.builder()
                .name("Service A")
                .email("update-a@example.com")
                .externalId("oauth-client-update-a")
                .build());
    userRepository.createUser(
        CreateUser.builder()
            .name("Service B")
            .email("update-b@example.com")
            .externalId("oauth-client-update-b")
            .build());

    assertThatThrownBy(
            () ->
                userRepository.updateUser(
                    first.getId(),
                    UpdateUser.builder().externalId("oauth-client-update-b").build()))
        .isInstanceOf(BaseException.class)
        .satisfies(
            e ->
                assertThat(((BaseException) e).getErrorCode()).isEqualTo(ErrorCode.ALREADY_EXISTS));
  }

  @Test
  void bootstrapCreatesTheAdminWhenTheLookupFindsNone() {
    UUID metastoreId = UUID.randomUUID();
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);

    bootstrap(userRepository, metastoreId).initializeAdmin(authorizer);

    var admin = userRepository.getUserByEmail("admin");
    verify(authorizer)
        .grantAuthorization(UUID.fromString(admin.getId()), metastoreId, Privileges.OWNER);
  }

  @Test
  void bootstrapFailsWhenTheAdminLookupFails() {
    UserRepository failing = spy(userRepository);
    BaseException lookupError = new BaseException(ErrorCode.INTERNAL, "Failed to get user");
    doThrow(lookupError).when(failing).getUserByEmail("admin");

    assertThatThrownBy(
            () ->
                bootstrap(failing, UUID.randomUUID())
                    .initializeAdmin(mock(UnityCatalogAuthorizer.class)))
        .isSameAs(lookupError);
    verify(failing, never()).createUser(any());
  }

  /** Bootstrap over {@code users}; the metastore is a mock, since only its ID is read. */
  private static UnityAccessUtil bootstrap(UserRepository users, UUID metastoreId) {
    MetastoreRepository metastoreRepository = mock(MetastoreRepository.class);
    when(metastoreRepository.getMetastoreId()).thenReturn(metastoreId);
    Repositories repositories = mock(Repositories.class);
    when(repositories.getUserRepository()).thenReturn(users);
    when(repositories.getMetastoreRepository()).thenReturn(metastoreRepository);
    return new UnityAccessUtil(repositories);
  }
}
