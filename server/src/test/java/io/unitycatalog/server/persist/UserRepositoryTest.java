package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.model.CreateUser;
import io.unitycatalog.server.persist.model.UpdateUser;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class UserRepositoryTest {

  private static SessionFactory sessionFactory;
  private static UserRepository userRepository;
  private static HibernateConfigurator hibernateConfigurator;

  @BeforeAll
  static void setUp() {
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    Properties hibernateProperties =
        HibernateConfigurator.setupHibernateProperties(new ServerProperties(properties));
    hibernateProperties.setProperty(
        "hibernate.connection.url", "jdbc:h2:mem:" + UUID.randomUUID() + ";IGNORECASE=TRUE");
    hibernateConfigurator = new HibernateConfigurator(hibernateProperties);
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
    var user = userRepository.getUserByEmail("human-a@example.com");
    assertThat(user.getIssuer()).isNull();
    assertThat(user.getSubject()).isNull();
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
  void oidcIdentityRoundTripsAndScopesSubjectToIssuer() {
    String issuer = "https://issuer-a.example.com";
    String otherIssuer = "https://issuer-b.example.com";
    var first = createOidcUser(issuer, "shared-subject");
    var second = createOidcUser(otherIssuer, "shared-subject");

    assertThat(userRepository.getUserByIssuerAndSubject(issuer, "shared-subject").getId())
        .isEqualTo(first.getId());
    assertThat(userRepository.getUserByIssuerAndSubject(otherIssuer, "shared-subject").getId())
        .isEqualTo(second.getId());

    var replacement = UpdateUser.builder().issuer(issuer).subject("new-subject").build();
    userRepository.updateUser(first.getId(), replacement);
    userRepository.updateUser(first.getId(), replacement);
    userRepository.updateUser(first.getId(), UpdateUser.builder().name("Renamed user").build());

    assertThat(userRepository.getUserByIssuerAndSubject(issuer, "new-subject").getId())
        .isEqualTo(first.getId());
    // The shared database uses a case-insensitive collation; OIDC lookup must still be exact.
    for (String[] missing :
        new String[][] {
          {issuer, "shared-subject"},
          {issuer.toUpperCase(Locale.ROOT), "new-subject"},
          {issuer, "NEW-SUBJECT"}
        }) {
      assertThatThrownBy(() -> userRepository.getUserByIssuerAndSubject(missing[0], missing[1]))
          .isInstanceOfSatisfying(
              BaseException.class,
              e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.NOT_FOUND));
    }
  }

  @Test
  void duplicateOidcIdentityIsRejectedOnCreateAndUpdate() {
    String issuer = "https://unique.example.com";
    var first = createOidcUser(issuer, "unique-subject");
    var second = createOidcUser(null, null);

    assertThatThrownBy(() -> createOidcUser(issuer, "unique-subject"))
        .isInstanceOfSatisfying(
            BaseException.class,
            e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.ALREADY_EXISTS))
        .hasMessage("User already exists with issuer: " + issuer + ", subject: unique-subject");
    assertThatThrownBy(
            () ->
                userRepository.updateUser(
                    second.getId(),
                    UpdateUser.builder().issuer(issuer).subject("unique-subject").build()))
        .isInstanceOfSatisfying(
            BaseException.class,
            e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.ALREADY_EXISTS));
    assertThat(userRepository.getUser(second.getId()).getIssuer()).isNull();

    // Bypass the repository's duplicate check to verify the database also enforces uniqueness.
    try (Session session = sessionFactory.openSession()) {
      var transaction = session.beginTransaction();
      try {
        UserDAO duplicate = UserDAO.from(first);
        duplicate.setId(UUID.randomUUID());
        duplicate.setEmail("racing@example.com");
        session.persist(duplicate);
        assertThatThrownBy(session::flush).isInstanceOf(ConstraintViolationException.class);
      } finally {
        transaction.rollback();
      }
    }
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "NULL, subject",
        "https://issuer.example.com, NULL",
        "'', subject",
        "' ', subject",
        "https://issuer.example.com, ''",
        "https://issuer.example.com, ' '"
      },
      nullValues = "NULL")
  void rejectsIncompleteOrBlankOidcIdentity(String issuer, String subject) {
    assertThatThrownBy(() -> createOidcUser(issuer, subject))
        .isInstanceOfSatisfying(
            BaseException.class,
            e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT))
        .hasMessage("Issuer and subject must both be non-blank or both omitted");

    var user = createOidcUser(null, null);
    assertThatThrownBy(
            () ->
                userRepository.updateUser(
                    user.getId(), UpdateUser.builder().issuer(issuer).subject(subject).build()))
        .isInstanceOfSatisfying(
            BaseException.class,
            e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT));
    assertThat(userRepository.getUser(user.getId()).getIssuer()).isNull();
    assertThat(userRepository.getUser(user.getId()).getSubject()).isNull();
  }

  private static User createOidcUser(String issuer, String subject) {
    return userRepository.createUser(
        CreateUser.builder()
            .name("OIDC user")
            .email(UUID.randomUUID() + "@example.com")
            .issuer(issuer)
            .subject(subject)
            .build());
  }
}
