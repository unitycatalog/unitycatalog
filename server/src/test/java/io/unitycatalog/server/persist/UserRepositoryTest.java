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

import io.unitycatalog.control.model.User;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.UnityAccessUtil;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.CreateUser;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.model.UpdateUser;
import io.unitycatalog.server.persist.model.UserLink;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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

  @Test
  void setIdpNameMarksTheUserAndKeepsIt() {
    var user =
        userRepository.createUser(CreateUser.builder().name("Okta").email("okta-scim").build());

    assertThat(userRepository.setIdpName("okta-scim", "okta").getId()).isEqualTo(user.getId());
    // Minting again for the same IdP is allowed.
    userRepository.setIdpName("okta-scim", "okta");
    assertThat(userRepository.getUser(user.getId()).getIdpName()).isEqualTo("okta");
    // A SCIM update leaves the mark in place.
    userRepository.updateUser(user.getId(), UpdateUser.builder().name("Okta SCIM").build());
    assertThat(userRepository.getUserByEmail("okta-scim").getIdpName()).isEqualTo("okta");
  }

  @Test
  void setIdpNameRefusesAMissingUserTheAdminAndAnotherIdp() {
    if (userRepository.listUsers(0, 100, u -> "admin".equals(u.getEmail())).isEmpty()) {
      userRepository.createUser(CreateUser.builder().name("admin").email("admin").build());
    }
    userRepository.createUser(CreateUser.builder().name("Entra").email("entra-scim").build());
    userRepository.setIdpName("entra-scim", "entra");

    assertSetIdpNameRefused("nobody-scim", ErrorCode.NOT_FOUND, "User not found: nobody-scim");
    assertSetIdpNameRefused(
        "admin", ErrorCode.INVALID_ARGUMENT, "The admin user can't be an IdP principal.");
    assertSetIdpNameRefused(
        "entra-scim",
        ErrorCode.FAILED_PRECONDITION,
        "User entra-scim is already the IdP principal of entra, not okta.");
    assertThat(userRepository.getUserByEmail("entra-scim").getIdpName()).isEqualTo("entra");
    assertThat(userRepository.getUserByEmail("admin").getIdpName()).isNull();
  }

  private static final String ISSUER = "https://idp.example.com";

  @Test
  void linkUserLinksAnUnlinkedEnabledRowOnceThenFindsItByTheLink() {
    var user =
        userRepository.createUser(
            CreateUser.builder().name("Link").email("link@example.com").build());
    UserLink link = new UserLink(ISSUER, "sub", "subject-link");

    assertThat(userRepository.findLinkedUser(link, null)).isNull();
    // A token without iat links a row that was never reactivated.
    assertThat(userRepository.linkUser("link@example.com", link, null).getId())
        .isEqualTo(user.getId());
    assertThat(userRepository.findLinkedUser(link, null).getId()).isEqualTo(user.getId());
    // The same claim value from another issuer, or as another claim kind, is another identity.
    assertThat(
            userRepository.findLinkedUser(
                new UserLink("https://other.example.com", "sub", "subject-link"), null))
        .isNull();
    assertThat(userRepository.findLinkedUser(new UserLink(ISSUER, "uid", "subject-link"), null))
        .isNull();
    // A linked row is never linked again, by another identity or by the same one.
    assertThat(
            userRepository.linkUser("link@example.com", new UserLink(ISSUER, "sub", "other"), null))
        .isNull();
    assertThat(userRepository.linkUser("link@example.com", link, null)).isNull();
    assertThat(userRepository.findLinkedUser(link, null).getId()).isEqualTo(user.getId());
  }

  @Test
  void findLinkedUserMatchesTheLinkExactlyOnAnyCollation() {
    // MySQL's default collation ignores case and accents; the test database's doesn't.
    boolean isMySql = testDatabase.jdbcUrl().startsWith("jdbc:mysql");
    if (isMySql) {
      alterLinkValueCollation("utf8mb4_0900_ai_ci");
    }
    try {
      var user =
          userRepository.createUser(
              CreateUser.builder().name("Alice").email("collation-link@example.com").build());
      userRepository.linkUser(
          "collation-link@example.com", new UserLink(ISSUER, "sub", "Alice"), null);

      assertThat(userRepository.findLinkedUser(new UserLink(ISSUER, "sub", "Alice"), null).getId())
          .isEqualTo(user.getId());
      assertThat(userRepository.findLinkedUser(new UserLink(ISSUER, "sub", "alice"), null))
          .isNull();
      assertThat(userRepository.findLinkedUser(new UserLink(ISSUER, "sub", "Alïce"), null))
          .isNull();
    } finally {
      if (isMySql) {
        alterLinkValueCollation("utf8mb4_0900_bin");
      }
    }
  }

  private static void alterLinkValueCollation(String collation) {
    try (var session = sessionFactory.openSession()) {
      var transaction = session.beginTransaction();
      session
          .createNativeMutationQuery(
              "ALTER TABLE uc_users MODIFY link_value varchar(255) COLLATE " + collation)
          .executeUpdate();
      transaction.commit();
    }
  }

  @Test
  void linkUserRefusesAMissingDisabledOrIdpPrincipalRow() {
    // The admin row is refused too; the external-mode journey in AuthServiceAllowlistTest checks
    // it, because creating "admin" here would break the bootstrap test above.
    var disabled =
        userRepository.createUser(
            CreateUser.builder().name("Disabled").email("disabled-link@example.com").build());
    userRepository.updateUser(disabled.getId(), UpdateUser.builder().active(false).build());
    userRepository.createUser(CreateUser.builder().name("Okta").email("okta-scim-link").build());
    userRepository.setIdpName("okta-scim-link", "okta");

    for (String email :
        new String[] {"nobody-link@example.com", "disabled-link@example.com", "okta-scim-link"}) {
      UserLink link = new UserLink(ISSUER, "sub", "refused-" + email);
      assertThat(userRepository.linkUser(email, link, null)).as(email).isNull();
      assertThat(userRepository.findLinkedUser(link, null)).as(email).isNull();
    }
  }

  @Test
  void reactivationClearsTheLinkAndOtherUpdatesKeepIt() {
    var user =
        userRepository.createUser(
            CreateUser.builder().name("Rehire").email("rehire@example.com").build());
    UserLink first = new UserLink(ISSUER, "uid", "00u-first");
    userRepository.linkUser("rehire@example.com", first, null);

    // Okta sends active=true on a routine update of an active user; that keeps the link.
    userRepository.updateUser(
        user.getId(), UpdateUser.builder().name("Rehired").active(true).build());
    assertThat(userRepository.findLinkedUser(first, null).getId()).isEqualTo(user.getId());
    // Deactivation keeps it too.
    userRepository.updateUser(user.getId(), UpdateUser.builder().active(false).build());
    assertThat(userRepository.findLinkedUser(first, null).getId()).isEqualTo(user.getId());

    // Reactivation clears it, so the next first call links the (possibly new) identity, but only
    // with a token issued at or after the reactivation: an old unexpired token, or one without iat,
    // can't link the row first.
    Instant beforeReactivation = Instant.now().minusSeconds(60);
    userRepository.updateUser(user.getId(), UpdateUser.builder().active(true).build());
    assertThat(userRepository.findLinkedUser(first, null)).isNull();
    assertThat(userRepository.linkUser("rehire@example.com", first, beforeReactivation)).isNull();
    assertThat(userRepository.linkUser("rehire@example.com", first, null)).isNull();
    assertThat(userRepository.findLinkedUser(first, null)).isNull();
    UserLink rehire = new UserLink(ISSUER, "uid", "00u-rehire");
    // iat has whole seconds, so a token issued in the second of the reactivation links.
    Instant issuedNow = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    assertThat(userRepository.linkUser("rehire@example.com", rehire, issuedNow).getId())
        .isEqualTo(user.getId());
    // The same rule holds on every later lookup of the link.
    assertThat(userRepository.findLinkedUser(rehire, issuedNow).getId()).isEqualTo(user.getId());
    assertThat(userRepository.findLinkedUser(rehire, beforeReactivation)).isNull();
    assertThat(userRepository.findLinkedUser(rehire, null)).isNull();
  }

  @Test
  void reactivationTimeIsAnInstantAcrossTheDaylightSavingFallBack() {
    // In Los Angeles 2026-11-01 01:00-02:00 occurs twice. A reactivation at 09:10Z (01:10 PST)
    // must refuse a token issued at 08:30Z (01:30 PDT), 40 minutes earlier. A wall-clock column
    // fails this only when the JVM runs in such a zone (-Duser.timezone=America/Los_Angeles).
    userRepository.createUser(CreateUser.builder().name("Dst").email("dst@example.com").build());
    UserLink link = new UserLink(ISSUER, "uid", "00u-dst");
    userRepository.linkUser("dst@example.com", link, null);
    setReactivatedEpochSecond("dst@example.com", Instant.parse("2026-11-01T09:10:00Z"));

    assertThat(userRepository.findLinkedUser(link, Instant.parse("2026-11-01T08:30:00Z"))).isNull();
    assertThat(userRepository.findLinkedUser(link, Instant.parse("2026-11-01T09:09:59Z"))).isNull();
    assertThat(userRepository.findLinkedUser(link, Instant.parse("2026-11-01T09:10:00Z")))
        .isNotNull();
  }

  /** Sets the row's reactivation time directly: the repository only sets it to the current time. */
  private static void setReactivatedEpochSecond(String email, Instant reactivatedAt) {
    try (var session = sessionFactory.openSession()) {
      var transaction = session.beginTransaction();
      session
          .createMutationQuery(
              "UPDATE UserDAO SET reactivatedEpochSecond = :at WHERE email = :email")
          .setParameter("at", reactivatedAt.getEpochSecond())
          .setParameter("email", email)
          .executeUpdate();
      transaction.commit();
    }
  }

  @Test
  void concurrentLinksOfOneIdentityToTwoRowsLinkOneRow() throws Exception {
    var rowA =
        userRepository.createUser(
            CreateUser.builder().name("A").email("race-a@example.com").build());
    var rowB =
        userRepository.createUser(
            CreateUser.builder().name("B").email("race-b@example.com").build());
    UserLink link = new UserLink(ISSUER, "sub", "racer-shared");
    int callers = 8;
    ExecutorService executor = Executors.newFixedThreadPool(callers);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<User>> results = new ArrayList<>();
      for (int i = 0; i < callers; i++) {
        String email = i % 2 == 0 ? "race-a@example.com" : "race-b@example.com";
        results.add(
            executor.submit(
                () -> {
                  start.await();
                  return userRepository.linkUser(email, link, null);
                }));
      }
      start.countDown();
      // The loser of uc_users_link_key gets null, not an exception.
      List<String> linked = new ArrayList<>();
      for (Future<User> result : results) {
        User user = result.get(60, TimeUnit.SECONDS);
        if (user != null) {
          linked.add(user.getId());
        }
      }
      assertThat(linked).hasSize(1);
      assertThat(userRepository.findLinkedUser(link, null).getId())
          .isEqualTo(linked.get(0))
          .isIn(rowA.getId(), rowB.getId());
    } finally {
      executor.shutdownNow();
    }
  }

  private static void assertSetIdpNameRefused(String email, ErrorCode code, String message) {
    assertThatThrownBy(() -> userRepository.setIdpName(email, "okta"))
        .isInstanceOf(BaseException.class)
        .hasMessage(message)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(code);
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
