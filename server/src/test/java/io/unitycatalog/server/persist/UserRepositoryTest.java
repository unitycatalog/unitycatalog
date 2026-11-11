package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
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
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class UserRepositoryTest {

  private static SessionFactory sessionFactory;
  private static UserRepository userRepository;
  @TempDatabase private static TestDatabase testDatabase;

  @BeforeAll
  static void setUp() {
    sessionFactory = testDatabase.sessionFactory();
    userRepository = new UserRepository(null, sessionFactory);
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
  void createUserConcurrentDuplicateEmailsCreateOneRow() throws Exception {
    int threads = 16;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      // The race is probabilistic, so repeat it with fresh emails.
      for (int round = 0; round < 5; round++) {
        String email = "race-" + round + "@example.com";
        CountDownLatch start = new CountDownLatch(1);
        List<Future<ErrorCode>> attempts = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
          attempts.add(
              executor.submit(
                  () -> {
                    start.await();
                    try {
                      userRepository.createUser(
                          CreateUser.builder().name("Racer").email(email).build());
                      return null;
                    } catch (BaseException e) {
                      return e.getErrorCode();
                    }
                  }));
        }
        start.countDown();
        List<ErrorCode> failures = new ArrayList<>();
        for (Future<ErrorCode> attempt : attempts) {
          failures.add(attempt.get());
        }

        assertThat(userRepository.listUsers(0, 1000, u -> email.equals(u.getEmail())))
            .as("rows for %s", email)
            .hasSize(1);
        assertThat(failures).filteredOn(code -> code != null).hasSize(threads - 1);
        assertThat(failures)
            .filteredOn(code -> code != null)
            .containsOnly(ErrorCode.ALREADY_EXISTS);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void updateUserChangesEmail() {
    var user =
        userRepository.createUser(
            CreateUser.builder().name("Mover").email("mover-old@example.com").build());

    var updated =
        userRepository.updateUser(
            user.getId(), UpdateUser.builder().email("mover-new@example.com").build());

    assertThat(updated.getId()).isEqualTo(user.getId());
    assertThat(userRepository.getUser(user.getId()).getEmail()).isEqualTo("mover-new@example.com");
    assertThat(userRepository.getUserByEmail("mover-new@example.com").getId())
        .isEqualTo(user.getId());
    assertThatThrownBy(() -> userRepository.getUserByEmail("mover-old@example.com"))
        .isInstanceOf(BaseException.class)
        .satisfies(
            e -> assertThat(((BaseException) e).getErrorCode()).isEqualTo(ErrorCode.NOT_FOUND));
  }

  @Test
  void updateUserRejectsDuplicateEmailOnAnotherUser() {
    var first =
        userRepository.createUser(
            CreateUser.builder().name("Clash A").email("clash-a@example.com").build());
    userRepository.createUser(
        CreateUser.builder().name("Clash B").email("clash-b@example.com").build());

    assertThatThrownBy(
            () ->
                userRepository.updateUser(
                    first.getId(), UpdateUser.builder().email("clash-b@example.com").build()))
        .isInstanceOf(BaseException.class)
        .satisfies(
            e -> {
              assertThat(((BaseException) e).getErrorCode()).isEqualTo(ErrorCode.ALREADY_EXISTS);
              assertThat(e).hasMessageContaining("User already exists: clash-b@example.com");
            });
    assertThat(userRepository.getUser(first.getId()).getEmail()).isEqualTo("clash-a@example.com");
  }

  @Test
  void updateUserLosingTheEmailRaceIsAlreadyExists() {
    var a =
        userRepository.createUser(
            CreateUser.builder().name("Race A").email("race-a@example.com").build());
    userRepository.createUser(
        CreateUser.builder().name("Race B").email("race-b@example.com").build());
    // The other writer has not committed yet, so the email pre-check finds nobody.
    UserRepository racing = spy(userRepository);
    doReturn(null).when(racing).getUserByEmail(any(Session.class), eq("race-b@example.com"));

    for (String externalId : Arrays.asList(null, "race-ext")) {
      assertThatThrownBy(
              () ->
                  racing.updateUser(
                      a.getId(),
                      UpdateUser.builder()
                          .email("race-b@example.com")
                          .externalId(externalId)
                          .build()))
          .isInstanceOf(BaseException.class)
          .satisfies(
              e -> {
                assertThat(((BaseException) e).getErrorCode()).isEqualTo(ErrorCode.ALREADY_EXISTS);
                assertThat(e).hasMessageContaining("User already exists: race-b@example.com");
              });
    }
  }

  @Test
  void deleteUserRemovesTheRowAndFreesTheEmail() {
    var user =
        userRepository.createUser(
            CreateUser.builder().name("Leaver").email("leaver@example.com").build());

    userRepository.deleteUser(user.getId());

    for (Runnable lookup :
        List.<Runnable>of(
            () -> userRepository.getUser(user.getId()),
            () -> userRepository.getUserByEmail("leaver@example.com"),
            () -> userRepository.deleteUser(user.getId()),
            () -> userRepository.updateUser(user.getId(), UpdateUser.builder().build()))) {
      assertThatThrownBy(lookup::run)
          .isInstanceOf(BaseException.class)
          .satisfies(
              e -> assertThat(((BaseException) e).getErrorCode()).isEqualTo(ErrorCode.NOT_FOUND));
    }
    var recreated =
        userRepository.createUser(
            CreateUser.builder().name("Leaver").email("leaver@example.com").build());
    assertThat(recreated.getId()).isNotEqualTo(user.getId());
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

  /** The metastore is a mock; only its ID is read. */
  private static UnityAccessUtil bootstrap(UserRepository users, UUID metastoreId) {
    MetastoreRepository metastoreRepository = mock(MetastoreRepository.class);
    when(metastoreRepository.getMetastoreId()).thenReturn(metastoreId);
    Repositories repositories = mock(Repositories.class);
    when(repositories.getUserRepository()).thenReturn(users);
    when(repositories.getMetastoreRepository()).thenReturn(metastoreRepository);
    return new UnityAccessUtil(repositories);
  }
}
