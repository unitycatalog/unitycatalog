package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.CreateUser;
import io.unitycatalog.server.persist.model.UpdateUser;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Properties;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class UserRepositoryTest {

  private static SessionFactory sessionFactory;
  private static UserRepository userRepository;

  @BeforeAll
  static void setUp() {
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    HibernateConfigurator hibernateConfigurator =
        new HibernateConfigurator(new ServerProperties(properties));
    sessionFactory = hibernateConfigurator.getSessionFactory();
    userRepository = new UserRepository(null, sessionFactory);
  }

  @AfterAll
  static void tearDown() {
    if (sessionFactory != null) {
      sessionFactory.close();
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
}
