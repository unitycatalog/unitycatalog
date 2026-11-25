package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.ManagedResourceType;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.dao.MetastoreDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.utils.NormalizedURL;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.hibernate.query.SelectionQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for ExternalLocationUtils including both unit tests for static helper methods and
 * integration tests that verify query generation with a real Hibernate session.
 */
public class ExternalLocationUtilsTest {

  @TempDir Path tempDir;

  @TempDatabase private TestDatabase testDatabase;
  private SessionFactory sessionFactory;
  private Session session;
  private ExternalLocationUtils externalLocationUtils;

  @BeforeEach
  public void setUp() {
    Properties properties = testDatabase.hibernateProperties();
    // A nested transaction cannot borrow a second connection while the caller is using this one.
    properties.setProperty("hibernate.connection.pool_size", "1");
    sessionFactory = testDatabase.openHibernateConfigurator(properties).getSessionFactory();
    session = sessionFactory.openSession();
    externalLocationUtils = new ExternalLocationUtils(sessionFactory);
  }

  @AfterEach
  public void tearDown() {
    if (session.getTransaction().isActive()) {
      session.getTransaction().rollback();
    }
    session.close();
  }

  /**
   * Helper method to validate that all URLs in the list produce the same expected parent paths.
   *
   * @param urls List of URLs to test
   * @param expectedResult Expected parent paths list
   */
  private void validateGetParentPathsList(List<String> urls, List<String> expectedResult) {
    for (String url : urls) {
      List<String> parentPathsList =
          ExternalLocationUtils.getParentPathsList(NormalizedURL.from(url)).stream()
              .map(NormalizedURL::toString)
              .toList();
      assertThat(parentPathsList).containsExactlyElementsOf(expectedResult);
    }
  }

  @Test
  public void testStillCovers() {
    NormalizedURL current = NormalizedURL.from("s3://b/a");
    for (String covering : List.of("s3://b/a", "s3://b", "s3://b/")) {
      assertThat(ExternalLocationUtils.stillCovers(NormalizedURL.from(covering), current))
          .as(covering)
          .isTrue();
    }
    for (String notCovering :
        List.of("s3://b/a/c", "s3://b/ab", "s3://b/a-b", "s3://c/a", "s3://c", "gs://b/a")) {
      assertThat(ExternalLocationUtils.stillCovers(NormalizedURL.from(notCovering), current))
          .as(notCovering)
          .isFalse();
    }
    assertThat(
            ExternalLocationUtils.stillCovers(
                NormalizedURL.from("s3://b/a"), NormalizedURL.from("s3://b/ab")))
        .isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = {"abfs", "abfss"})
  public void azureSchemesShareOverlapAndCleanupReservations(String storedScheme) {
    session.beginTransaction();
    String stored = storedScheme + "://container@account.dfs.core.windows.net/warehouse/orders";
    String alias =
        (storedScheme.equals("abfs") ? "abfss" : "abfs")
            + "://container@account.dfs.core.windows.net/warehouse/orders";
    TableInfoDAO table =
        TableInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("orders")
            .type("MANAGED")
            .dataSourceFormat("ICEBERG")
            .url(stored)
            .build();
    session.persist(table);
    for (String path :
        List.of(alias, alias + "/data", alias.substring(0, alias.lastIndexOf('/')))) {
      NormalizedURL location = NormalizedURL.from(path);
      assertThat(
              ExternalLocationUtils.<TableInfoDAO>getEntityDAOsWithURLOverlap(
                  session, location, SecurableType.TABLE, 1, true, true, true))
          .extracting(TableInfoDAO::getId)
          .containsExactly(table.getId());
      assertInvalidRegistrationOverlap(location);
    }
    assertThat(
            ExternalLocationUtils.stillCovers(
                NormalizedURL.from(stored), NormalizedURL.from(alias)))
        .isTrue();
    assertThat(
            ExternalLocationUtils.stillCovers(
                NormalizedURL.from(stored), NormalizedURL.from(alias + "/data")))
        .isTrue();
    assertThat(
            ExternalLocationUtils.stillCovers(
                NormalizedURL.from(stored), NormalizedURL.from(alias + "-sibling")))
        .isFalse();

    session.remove(table);
    StorageCleanupTaskDAO task =
        new StorageCleanupTaskRepository(sessionFactory)
            .create(session, ManagedResourceType.REGISTERED_TABLE, table.getId(), "orders", stored);
    for (boolean completed : List.of(false, true)) {
      if (completed) {
        task.setCompletedAt(new Date());
      }
      NormalizedURL location = NormalizedURL.from(alias);
      assertThat(ExternalLocationUtils.hasPendingCleanupOverlap(session, location))
          .isEqualTo(!completed);
      assertInvalidRegistrationOverlap(location);
    }
    for (String path : List.of(alias + "-sibling", alias.replace("container@", "other@"))) {
      NormalizedURL location = NormalizedURL.from(path);
      assertThat(ExternalLocationUtils.hasPendingCleanupOverlap(session, location)).isFalse();
      ExternalLocationUtils.validateNotOverlapWithRegisteredTables(session, location);
    }
  }

  private void assertInvalidRegistrationOverlap(NormalizedURL location) {
    assertThatThrownBy(
            () -> ExternalLocationUtils.validateNotOverlapWithRegisteredTables(session, location))
        .isInstanceOfSatisfying(
            BaseException.class,
            failure -> {
              assertThat(failure.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
              assertThat(failure).hasMessageContaining("overlaps a registered managed table");
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"case", "symlink"})
  public void localAliasesReserveRegisteredStorageAndCleanup(String kind) throws Exception {
    Path parent = Files.createDirectory(tempDir.resolve("CaseStorage"));
    Path directory = Files.createDirectory(parent.resolve("Orders"));
    Path aliasParent = tempDir.resolve(kind.equals("case") ? "casestorage" : "alias");
    if (kind.equals("case")) {
      assumeTrue(
          Files.exists(aliasParent) && Files.isSameFile(parent, aliasParent),
          "Requires a case-insensitive filesystem");
    } else {
      Files.createSymbolicLink(aliasParent, parent);
    }
    session.beginTransaction();
    TableInfoDAO table =
        TableInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("orders")
            .type("MANAGED")
            .dataSourceFormat("ICEBERG")
            .url(NormalizedURL.from(directory.toUri()).toString())
            .build();
    session.persist(table);
    String aliasName = kind.equals("case") ? "orders" : "Orders";
    List<NormalizedURL> aliases =
        List.of(
            NormalizedURL.from(aliasParent.resolve(aliasName).toUri()),
            NormalizedURL.from(aliasParent.resolve(aliasName + "/not-created/data").toUri()),
            NormalizedURL.from(aliasParent.toUri()));
    aliases.forEach(this::assertInvalidRegistrationOverlap);
    ExternalLocationUtils.validateNotOverlapWithRegisteredTables(
        session, NormalizedURL.from(aliasParent.resolve("orders-sibling").toUri()));
    session.remove(table);
    StorageCleanupTaskDAO task =
        new StorageCleanupTaskRepository(sessionFactory)
            .create(
                session,
                ManagedResourceType.REGISTERED_TABLE,
                table.getId(),
                "orders",
                table.getUrl());
    aliases.forEach(this::assertInvalidRegistrationOverlap);
    task.setCompletedAt(new Date());
    aliases.forEach(this::assertInvalidRegistrationOverlap);
    Files.delete(directory);
    aliases.forEach(this::assertInvalidRegistrationOverlap);
  }

  @Test
  public void localStorageOverlapPreservesDistinctExistingDirectories() throws Exception {
    Path first = Files.createDirectory(tempDir.resolve("CasePath"));
    Path second = Files.createDirectories(tempDir.resolve("casepath"));
    assertThat(LocalStorageLocationValidator.storagePathsOverlap(first, second))
        .isEqualTo(Files.isSameFile(first, second));
    assertThat(
            LocalStorageLocationValidator.storagePathsOverlap(
                first, tempDir.resolve("unrelated/not-created")))
        .isFalse();
    assertThat(
            LocalStorageLocationValidator.storagePathsOverlap(
                tempDir.resolve("Missing/child"), tempDir.resolve("missing")))
        .isTrue();
  }

  @Test
  public void localStorageOverlapFailsClosedWhenIdentityCannotBeRead() throws Exception {
    Path unreadable = mock(Path.class);
    when(unreadable.toRealPath()).thenThrow(new IOException("Read failed"));
    assertThatThrownBy(() -> LocalStorageLocationValidator.storagePathsOverlap(unreadable, tempDir))
        .isInstanceOfSatisfying(
            BaseException.class,
            failure -> {
              assertThat(failure.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
              assertThat(failure)
                  .hasMessageContaining("Cannot resolve local storage path")
                  .hasCauseInstanceOf(IOException.class);
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"table", "model"})
  public void registrationCannotContainDefaultAllocationStorage(String kind) {
    session.beginTransaction();
    session.persist(
        ExternalLocationDAO.builder()
            .id(UUID.randomUUID())
            .name("warehouse")
            .url("s3://bucket")
            .credentialId(UUID.randomUUID())
            .build());
    var parents =
        new RepositoryUtils.CatalogAndSchemaDao(
            CatalogInfoDAO.builder().id(UUID.randomUUID()).build(),
            SchemaInfoDAO.builder().id(UUID.randomUUID()).build());
    NormalizedURL root = NormalizedURL.from("s3://bucket/warehouse");
    Optional<NormalizedURL> tableRoot = kind.equals("table") ? Optional.of(root) : Optional.empty();
    Optional<NormalizedURL> modelRoot = kind.equals("model") ? Optional.of(root) : Optional.empty();
    for (String path : List.of("s3://bucket", root.toString(), root + "/__unitystorage")) {
      assertThatThrownBy(
              () ->
                  ExternalLocationUtils.resolveRegistrationStorage(
                      session, parents, NormalizedURL.from(path), tableRoot, modelRoot))
          .isInstanceOfSatisfying(
              BaseException.class,
              failure -> {
                assertThat(failure.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
                assertThat(failure)
                    .hasMessageContaining(
                        "must not contain or equal a shared managed storage directory");
              });
    }
    assertThat(
            ExternalLocationUtils.resolveRegistrationStorage(
                    session, parents, NormalizedURL.from(root + "/imported"), tableRoot, modelRoot)
                .location())
        .isEqualTo(NormalizedURL.from("s3://bucket"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"default", "catalog", "schema"})
  public void registrationReservesLocalSharedDirectoriesWithoutAllocationRows(String owner)
      throws Exception {
    session.beginTransaction();
    Path managedDirectory = Files.createDirectory(tempDir.resolve("__unitystorage"));
    NormalizedURL managedRoot = NormalizedURL.from(managedDirectory.toUri());
    CatalogInfoDAO catalog = CatalogInfoDAO.builder().id(UUID.randomUUID()).build();
    SchemaInfoDAO schema = SchemaInfoDAO.builder().id(UUID.randomUUID()).build();
    if (owner.equals("catalog")) {
      catalog.setStorageLocation(managedRoot.toString());
    } else if (owner.equals("schema")) {
      schema.setStorageLocation(managedRoot.toString());
    }
    var parents = new RepositoryUtils.CatalogAndSchemaDao(catalog, schema);
    Optional<NormalizedURL> defaultRoot =
        owner.equals("default")
            ? Optional.of(NormalizedURL.from(tempDir.toUri()))
            : Optional.empty();
    for (String segment : List.of("tables", "volumes", "models", "catalogs", "schemas")) {
      Path reserved = Files.createDirectory(managedDirectory.resolve(segment));
      Path alias =
          Files.createDirectories(managedDirectory.resolve(segment.toUpperCase(Locale.ROOT)));
      for (Path spelling : List.of(reserved, alias)) {
        boolean isReserved = Files.isSameFile(reserved, spelling);
        for (Path path : List.of(spelling, spelling.resolve("child"))) {
          if (isReserved) {
            assertThatThrownBy(
                    () ->
                        ExternalLocationUtils.resolveRegistrationStorage(
                            session,
                            parents,
                            NormalizedURL.from(path.toUri()),
                            defaultRoot,
                            Optional.empty()))
                .isInstanceOfSatisfying(
                    BaseException.class,
                    failure -> {
                      assertThat(failure.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
                      assertThat(failure).hasMessageContaining("shared managed storage directory");
                    });
          } else {
            assertThat(
                    ExternalLocationUtils.resolveRegistrationStorage(
                            session,
                            parents,
                            NormalizedURL.from(path.toUri()),
                            defaultRoot,
                            Optional.empty())
                        .location())
                .isEqualTo(managedRoot);
          }
        }
      }
    }
    assertThat(
            ExternalLocationUtils.resolveRegistrationStorage(
                    session,
                    parents,
                    NormalizedURL.from(managedDirectory.resolve("tables-sibling").toUri()),
                    defaultRoot,
                    Optional.empty())
                .location())
        .isEqualTo(managedRoot);
  }

  @ParameterizedTest
  @ValueSource(strings = {"table", "model"})
  public void registrationRejectsCaseAliasesOfDefaultAllocationStorage(String kind)
      throws Exception {
    Path managedDirectory = Files.createDirectory(tempDir.resolve("__unitystorage"));
    Path alias = tempDir.resolve("__UNITYSTORAGE");
    assumeTrue(
        Files.exists(alias) && Files.isSameFile(managedDirectory, alias),
        "Requires a case-insensitive filesystem");
    session.beginTransaction();
    var parents =
        new RepositoryUtils.CatalogAndSchemaDao(
            CatalogInfoDAO.builder().id(UUID.randomUUID()).build(),
            SchemaInfoDAO.builder().id(UUID.randomUUID()).build());
    NormalizedURL root = NormalizedURL.from(tempDir.toUri());
    Optional<NormalizedURL> tableRoot = kind.equals("table") ? Optional.of(root) : Optional.empty();
    Optional<NormalizedURL> modelRoot = kind.equals("model") ? Optional.of(root) : Optional.empty();
    for (Path path : List.of(alias, alias.resolve("child"))) {
      assertThatThrownBy(
              () ->
                  ExternalLocationUtils.resolveRegistrationStorage(
                      session, parents, NormalizedURL.from(path.toUri()), tableRoot, modelRoot))
          .isInstanceOfSatisfying(
              BaseException.class,
              failure -> {
                assertThat(failure.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
                assertThat(failure).hasMessageContaining("shared managed storage directory");
              });
    }
  }

  @ParameterizedTest
  @MethodSource("io.unitycatalog.server.persist.utils.RepositoryUtilsTest#lockAcquisitionFailures")
  @SuppressWarnings("unchecked")
  public void storageLockFailureIsRetryable(RuntimeException lockFailure) {
    Session lockedSession = mock();
    SelectionQuery<MetastoreDAO> query = mock(SelectionQuery.class, RETURNS_SELF);
    when(lockedSession.createSelectionQuery("FROM MetastoreDAO", MetastoreDAO.class))
        .thenReturn(query);
    when(query.getSingleResult()).thenThrow(lockFailure);
    assertThatThrownBy(() -> ExternalLocationUtils.lockStorageAllocation(lockedSession))
        .isInstanceOfSatisfying(
            BaseException.class,
            failure -> {
              assertThat(failure.getErrorCode()).isEqualTo(ErrorCode.UNAVAILABLE);
              assertThat(failure.getErrorCode().getHttpStatus().code()).isEqualTo(503);
              assertThat(failure)
                  .hasCause(lockFailure)
                  .hasMessage(
                      "Concurrent modification in progress on managed storage; retry the request.");
            });
  }

  @Test
  public void testPendingCleanupIsRejectedBeforePathFallback() {
    session.beginTransaction();
    ExternalLocationDAO externalLocation =
        ExternalLocationDAO.builder()
            .id(UUID.randomUUID())
            .name("external")
            .url("s3://bucket/external")
            .credentialId(UUID.randomUUID())
            .build();
    session.persist(externalLocation);
    StorageCleanupTaskDAO task = createCleanupTask("s3://bucket/external/deleted");
    session.getTransaction().commit();

    for (String path :
        List.of(
            externalLocation.getUrl(),
            task.getStorageLocation(),
            task.getStorageLocation() + "/data")) {
      NormalizedURL url = NormalizedURL.from(path);
      assertPendingCleanupDenied(() -> externalLocationUtils.getMapResourceIdsForPath(url));
    }
    assertThat(
            externalLocationUtils.getMapResourceIdsForPath(
                NormalizedURL.from("s3://bucket/external/active")))
        .containsOnlyKeys(SecurableType.EXTERNAL_LOCATION)
        .containsEntry(SecurableType.EXTERNAL_LOCATION, externalLocation.getId());

    session.beginTransaction();
    session.remove(task);
    session.getTransaction().commit();
    NormalizedURL releasedPath = NormalizedURL.from(task.getStorageLocation());
    assertThat(externalLocationUtils.getMapResourceIdsForPath(releasedPath))
        .containsOnlyKeys(SecurableType.EXTERNAL_LOCATION)
        .containsEntry(SecurableType.EXTERNAL_LOCATION, externalLocation.getId());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "s3://bucket",
        "gs://bucket",
        "abfs://c@a.dfs.core.windows.net",
        "abfss://c@a.dfs.core.windows.net"
      })
  public void cloudAllocationAndPathCredentialsRejectAmbiguousUris(String storage) {
    for (String suffix :
        List.of("/%6frders", "/orders%2fchild", "/orders?query=1", "/orders#fragment")) {
      NormalizedURL path = NormalizedURL.from(storage + "/root" + suffix);
      for (Runnable operation :
          List.<Runnable>of(
              () -> externalLocationUtils.getMapResourceIdsForPath(path),
              () -> ExternalLocationUtils.validateNotOverlapWithRegisteredTables(session, path))) {
        assertThatThrownBy(operation::run)
            .isInstanceOfSatisfying(
                BaseException.class,
                e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT))
            .hasMessageContaining("must not contain percent escapes, a query, or a fragment");
      }
    }
  }

  @Test
  public void completedReservationAllowsLifecycleAndDisjointNamespaceAllocation() {
    session.beginTransaction();
    ExternalLocationDAO external =
        ExternalLocationDAO.builder()
            .id(UUID.randomUUID())
            .name("warehouse")
            .url("s3://bucket/warehouse")
            .credentialId(UUID.randomUUID())
            .build();
    session.persist(external);
    StorageCleanupTaskDAO task =
        new StorageCleanupTaskRepository(sessionFactory)
            .create(
                session,
                ManagedResourceType.REGISTERED_TABLE,
                UUID.randomUUID(),
                "orders",
                external.getUrl() + "/orders");
    task.setCompletedAt(new Date());
    session.getTransaction().commit();
    NormalizedURL root = NormalizedURL.from(external.getUrl());
    NormalizedURL retired = NormalizedURL.from(task.getStorageLocation());
    assertThat(externalLocationUtils.getMapResourceIdsForPath(root, true))
        .containsEntry(SecurableType.EXTERNAL_LOCATION, external.getId());
    for (String path : List.of(retired.toString(), retired + "/child")) {
      assertPendingCleanupDenied(
          () -> externalLocationUtils.getMapResourceIdsForPath(NormalizedURL.from(path), true));
    }
    // The same parent must still refuse broad read/write path credentials.
    assertPendingCleanupDenied(() -> externalLocationUtils.getMapResourceIdsForPath(root));
    session.beginTransaction();
    assertThat(ExternalLocationUtils.hasPendingCleanupOverlap(session, root)).isFalse();
    assertThat(ExternalLocationUtils.findLocationUse(session, root).inUse()).isFalse();
    ExternalLocationUtils.validateExternalLocationNotInUse(session, root);
    ExternalLocationUtils.validateNotOverlapWithRegisteredTables(
        session, NormalizedURL.from(root + "/__unitystorage/catalogs/" + UUID.randomUUID()));
    assertThatThrownBy(
            () -> ExternalLocationUtils.validateNotOverlapWithRegisteredTables(session, retired))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("retired");
  }

  @Test
  public void testPendingCleanupUsesCallerTransaction() {
    session.beginTransaction();
    for (String base :
        List.of(
            NormalizedURL.from(tempDir.toUri().toString()).toString(),
            "s3://bucket",
            "gs://bucket",
            "abfs://container@account.dfs.core.windows.net",
            "abfss://container@account.dfs.core.windows.net")) {
      String location = base + "/reserved/deleted";
      StorageCleanupTaskDAO task = createCleanupTask(location);
      for (String overlappingPath :
          List.of(
              base.startsWith("file:") ? "file:///" : base,
              base + "/reserved",
              location,
              location + "/",
              location + "/data",
              base + "/reserved/unused/../deleted")) {
        assertPendingCleanupDenied(
            () ->
                externalLocationUtils.getMapResourceIdsForPath(
                    session, NormalizedURL.from(overlappingPath)));
      }

      for (String unrelatedPath :
          List.of(location + "-sibling", base + "/other", base + "/RESERVED/deleted")) {
        assertThat(
                externalLocationUtils.getMapResourceIdsForPath(
                    session, NormalizedURL.from(unrelatedPath)))
            .isEmpty();
      }
      session.remove(task);
      assertThat(
              externalLocationUtils.getMapResourceIdsForPath(session, NormalizedURL.from(location)))
          .isEmpty();
    }
  }

  @Test
  public void testLocalRootSubdirectoryQueryExcludesRootItself() {
    session.beginTransaction();
    String child = NormalizedURL.from(tempDir.toUri().toString()).toString();
    for (String url : List.of("file:///", child)) {
      session.persist(
          ExternalLocationDAO.builder()
              .id(UUID.randomUUID())
              .name("location")
              .url(url)
              .credentialId(UUID.randomUUID())
              .build());
    }
    assertThat(
            ExternalLocationUtils.<ExternalLocationDAO>getEntityDAOsWithURLOverlap(
                session,
                NormalizedURL.from("file:///"),
                SecurableType.EXTERNAL_LOCATION,
                /* limit= */ 10,
                /* includeParent= */ false,
                /* includeSelf= */ false,
                /* includeSubdir= */ true))
        .extracting(ExternalLocationDAO::getUrl)
        .containsExactly(child);
  }

  @Test
  public void testListLocalExternalLocationUrls() {
    session.beginTransaction();
    String externalLocationUrl = NormalizedURL.from(tempDir.resolve("el").toUri()).toString();
    for (String url : List.of(externalLocationUrl, "s3://bucket/el")) {
      session.persist(
          ExternalLocationDAO.builder()
              .id(UUID.randomUUID())
              .name("external_" + url.length())
              .url(url)
              .credentialId(UUID.randomUUID())
              .build());
    }
    session.getTransaction().commit();

    assertThat(externalLocationUtils.listLocalExternalLocationUrls())
        .containsExactly(externalLocationUrl);
  }

  @Test
  public void testPendingCleanupCoversEveryResourceType() {
    session.beginTransaction();
    StorageCleanupTaskRepository repository = new StorageCleanupTaskRepository(sessionFactory);
    for (ManagedResourceType resourceType : ManagedResourceType.values()) {
      NormalizedURL location = NormalizedURL.from("s3://bucket/" + resourceType);
      repository.create(
          session, resourceType, UUID.randomUUID(), "deleted_resource", location.toString());
      assertPendingCleanupDenied(
          () -> externalLocationUtils.getMapResourceIdsForPath(session, location));
    }
  }

  @Test
  public void testPendingCleanupChecksFullPathsAndLiteralWildcards() {
    session.beginTransaction();
    String commonPrefix = "s3://bucket/" + "a".repeat(800);
    createCleanupTask(commonPrefix + "/stored");
    for (String path :
        List.of(commonPrefix, commonPrefix + "/stored", commonPrefix + "/stored/child")) {
      assertPendingCleanupDenied(
          () -> externalLocationUtils.getMapResourceIdsForPath(session, NormalizedURL.from(path)));
    }
    assertThat(
            externalLocationUtils.getMapResourceIdsForPath(
                session, NormalizedURL.from(commonPrefix + "/sibling")))
        .isEmpty();

    createCleanupTask("s3://bucket/literalX/child");
    createCleanupTask("s3://bucket/percentXYZ25/child");
    for (String path : List.of("s3://bucket/literal_", "s3://bucket/percent%25")) {
      assertThat(ExternalLocationUtils.hasPendingCleanupOverlap(session, NormalizedURL.from(path)))
          .isFalse();
      createCleanupTask(path + "/child");
      assertThat(ExternalLocationUtils.hasPendingCleanupOverlap(session, NormalizedURL.from(path)))
          .isTrue();
    }
  }

  private StorageCleanupTaskDAO createCleanupTask(String location) {
    return new StorageCleanupTaskRepository(sessionFactory)
        .create(session, ManagedResourceType.TABLE, UUID.randomUUID(), "orders", location);
  }

  private static void assertPendingCleanupDenied(Runnable action) {
    assertThatThrownBy(action::run)
        .isInstanceOfSatisfying(
            BaseException.class,
            exception -> {
              assertThat(exception.getErrorCode()).isEqualTo(ErrorCode.PERMISSION_DENIED);
              assertThat(exception)
                  .hasMessage(
                      "Input path overlaps pending storage cleanup or a retired registered"
                          + " directory.");
            });
  }

  @Test
  public void testGetParentAndCurrentPathsList() {
    // Test S3 nested path
    validateGetParentPathsList(
        List.of(
            "s3://bucket/path/to/file",
            "s3://bucket/path/to/file/",
            "s3://bucket/path/to/file///",
            "s3://bucket/path////to/file/",
            "s3://bucket///path/to/file//"),
        List.of("s3://bucket/path/to", "s3://bucket/path", "s3://bucket"));

    // Test S3 single path level
    validateGetParentPathsList(
        List.of(
            "s3://bucket/path", "s3://bucket/path/", "s3://bucket/path//", "s3://bucket/path///"),
        List.of("s3://bucket"));

    // Test S3 bucket only (no path)
    validateGetParentPathsList(
        List.of("s3://bucket", "s3://bucket/", "s3://bucket//", "s3://bucket///"), List.of());

    // Test Azure Blob Storage nested path
    validateGetParentPathsList(
        List.of(
            "abfs://container@storage.dfs.core.windows.net/path/to/file",
            "abfs://container@storage.dfs.core.windows.net/path/to/file/",
            "abfs://container@storage.dfs.core.windows.net/path/to/file///",
            "abfs://container@storage.dfs.core.windows.net/path////to/file/",
            "abfs://container@storage.dfs.core.windows.net///path/to/file//"),
        List.of(
            "abfs://container@storage.dfs.core.windows.net/path/to",
            "abfs://container@storage.dfs.core.windows.net/path",
            "abfs://container@storage.dfs.core.windows.net"));

    // Test Azure Blob Storage single path level
    validateGetParentPathsList(
        List.of(
            "abfs://container@storage.dfs.core.windows.net/path",
            "abfs://container@storage.dfs.core.windows.net/path/",
            "abfs://container@storage.dfs.core.windows.net/path//",
            "abfs://container@storage.dfs.core.windows.net/path///"),
        List.of("abfs://container@storage.dfs.core.windows.net"));

    // Test Azure Blob Storage container only (no path)
    validateGetParentPathsList(
        List.of(
            "abfs://container@storage.dfs.core.windows.net",
            "abfs://container@storage.dfs.core.windows.net/",
            "abfs://container@storage.dfs.core.windows.net//",
            "abfs://container@storage.dfs.core.windows.net///"),
        List.of());

    // Test Google Cloud Storage nested path
    validateGetParentPathsList(
        List.of(
            "gs://bucket/path/to/file",
            "gs://bucket/path/to/file/",
            "gs://bucket/path/to/file///",
            "gs://bucket/path////to/file/",
            "gs://bucket///path/to/file//"),
        List.of("gs://bucket/path/to", "gs://bucket/path", "gs://bucket"));

    // Test Google Cloud Storage single path level
    validateGetParentPathsList(
        List.of(
            "gs://bucket/path", "gs://bucket/path/", "gs://bucket/path//", "gs://bucket/path///"),
        List.of("gs://bucket"));

    // Test Google Cloud Storage bucket only (no path)
    validateGetParentPathsList(
        List.of("gs://bucket", "gs://bucket/", "gs://bucket//", "gs://bucket///"), List.of());

    // Test file:// nested path
    validateGetParentPathsList(
        List.of(
            "file:///path/to/file",
            "file:///path/to/file/",
            "file:///path/to/file///",
            "file:///path////to/file/",
            "file://///path/to/file//"),
        List.of("file:///path/to", "file:///path", "file:///"));

    // Test file:// single path level
    validateGetParentPathsList(
        List.of(
            "file:///path",
            "file:///path/",
            "file:///path//",
            "file:////path///",
            "file:///path///",
            "file://path///",
            "file:/path///",
            "file:///path///"),
        List.of("file:///"));

    // Test file:// root only
    validateGetParentPathsList(List.of("file:/", "file:///", "file:////", "file://///"), List.of());

    // Test local FS nested path
    validateGetParentPathsList(
        List.of(
            "/path/to/file",
            "/path/to/file/",
            "/path/to/file///",
            "/path////to/file/",
            "///path/to/file//"),
        List.of("file:///path/to", "file:///path", "file:///"));

    // Test local FS single path level
    validateGetParentPathsList(
        List.of("/path", "/path/", "/path//", "//path//", "///path///", "/path///"),
        List.of("file:///"));

    // Test local FS root only
    validateGetParentPathsList(List.of("/", "///", "/////"), List.of());

    // Test deeply nested path (10 levels)
    validateGetParentPathsList(
        List.of("s3://bucket/a/b/c/d/e/f/g/h/i/j"),
        List.of(
            "s3://bucket/a/b/c/d/e/f/g/h/i",
            "s3://bucket/a/b/c/d/e/f/g/h",
            "s3://bucket/a/b/c/d/e/f/g",
            "s3://bucket/a/b/c/d/e/f",
            "s3://bucket/a/b/c/d/e",
            "s3://bucket/a/b/c/d",
            "s3://bucket/a/b/c",
            "s3://bucket/a/b",
            "s3://bucket/a",
            "s3://bucket"));
  }

  @Test
  public void testEscapeLikePattern() {
    // Test string with no special characters
    String noSpecialResult = ExternalLocationUtils.escapeLikePattern("s3://bucket/path");
    assertThat(noSpecialResult).isEqualTo("s3://bucket/path");

    // Test string with percent sign
    String percentResult = ExternalLocationUtils.escapeLikePattern("s3://bucket/path%test");
    assertThat(percentResult).isEqualTo("s3://bucket/path\\%test");

    // Test string with underscore
    String underscoreResult = ExternalLocationUtils.escapeLikePattern("s3://bucket/path_test");
    assertThat(underscoreResult).isEqualTo("s3://bucket/path\\_test");

    // Test string with backslash
    String backslashResult = ExternalLocationUtils.escapeLikePattern("s3://bucket/path\\test");
    assertThat(backslashResult).isEqualTo("s3://bucket/path\\\\test");

    // Test string with all special characters
    String allSpecialResult = ExternalLocationUtils.escapeLikePattern("s3://bucket/a%b_c\\d");
    assertThat(allSpecialResult).isEqualTo("s3://bucket/a\\%b\\_c\\\\d");

    // Test empty string
    String emptyResult = ExternalLocationUtils.escapeLikePattern("");
    assertThat(emptyResult).isEqualTo("");
  }

  @Test
  public void testUnsupportedSecurableType() {
    // METASTORE, CREDENTIAL, FUNCTION are not supported for URL overlap checks
    assertThatThrownBy(
            () ->
                ExternalLocationUtils.getEntityDAOsWithURLOverlap(
                    null,
                    NormalizedURL.from("s3://bucket/path"),
                    SecurableType.METASTORE,
                    /* limit= */ 1,
                    /* includeParent= */ true,
                    /* includeSelf= */ true,
                    /* includeSubdir= */ true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported securable type for URL overlap check");

    assertThatThrownBy(
            () ->
                ExternalLocationUtils.getEntityDAOsWithURLOverlap(
                    null,
                    NormalizedURL.from("s3://bucket/path"),
                    SecurableType.CREDENTIAL,
                    /* limit= */ 1,
                    /* includeParent= */ true,
                    /* includeSelf= */ true,
                    /* includeSubdir= */ true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported securable type for URL overlap check");

    assertThatThrownBy(
            () ->
                ExternalLocationUtils.getEntityDAOsWithURLOverlap(
                    null,
                    NormalizedURL.from("s3://bucket/path"),
                    SecurableType.FUNCTION,
                    /* limit= */ 1,
                    /* includeParent= */ true,
                    /* includeSelf= */ true,
                    /* includeSubdir= */ true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported securable type for URL overlap check");
  }

  /**
   * Validates a query generated by {@link
   * ExternalLocationUtils#generateEntitiesDAOsWithURLOverlapQuery} that it has exactly the same
   * query string and parameters.
   *
   * @param query The query to be validated.
   * @param expectedQueryString The query must have this query string.
   * @param expectedMatchPaths They query must have this as :matchPaths parameter. Or it must have
   *     no :matchPaths parameter if expectedMatchPaths is null.
   * @param expectedLikePattern They query must have this as :likePattern parameter. Or it must have
   *     no :likePattern parameter if expectedLikePattern is null.
   */
  private void validateQuery(
      Query<ExternalLocationDAO> query,
      String expectedQueryString,
      List<String> expectedMatchPaths,
      String expectedLikePattern) {
    String queryString = query.getQueryString();
    assertThat(queryString).isEqualTo(expectedQueryString);
    if (expectedMatchPaths != null) {
      List<String> matchPaths1 = (List<String>) query.getParameterValue("matchPaths");
      assertThat(matchPaths1).isEqualTo(expectedMatchPaths);
    } else {
      assertThatThrownBy(() -> query.getParameterValue("matchPaths"))
          .isInstanceOf(IllegalArgumentException.class);
    }
    if (expectedLikePattern != null) {
      String likePattern = (String) query.getParameterValue("likePattern");
      assertThat(likePattern).isEqualTo(expectedLikePattern);
    } else {
      assertThatThrownBy(() -> query.getParameterValue("likePattern"))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  public void testGenerateEntitiesDAOsWithURLOverlapQueryWithRealSession() {
    // Test 1: includeSelf only
    Query<ExternalLocationDAO> query1 =
        ExternalLocationUtils.generateEntitiesDAOsWithURLOverlapQuery(
            session,
            NormalizedURL.from("s3://bucket/path"),
            SecurableType.EXTERNAL_LOCATION,
            /* limit= */ 10,
            /* includeParent= */ false,
            /* includeSelf= */ true,
            /* includeSubdir= */ false);

    validateQuery(
        query1,
        "FROM ExternalLocationDAO WHERE url IN (:matchPaths) ORDER BY LENGTH(url) DESC",
        List.of("s3://bucket/path"),
        null);

    // Test 2: includeParent only
    Query<ExternalLocationDAO> query2 =
        ExternalLocationUtils.generateEntitiesDAOsWithURLOverlapQuery(
            session,
            NormalizedURL.from("s3://bucket/path/to/file"),
            SecurableType.EXTERNAL_LOCATION,
            /* limit= */ 10,
            /* includeParent= */ true,
            /* includeSelf= */ false,
            /* includeSubdir= */ false);

    validateQuery(
        query2,
        "FROM ExternalLocationDAO WHERE url IN (:matchPaths) ORDER BY LENGTH(url) DESC",
        List.of("s3://bucket/path/to", "s3://bucket/path", "s3://bucket"),
        null);

    // Test 3: includeSubdir only
    Query<ExternalLocationDAO> query3 =
        ExternalLocationUtils.generateEntitiesDAOsWithURLOverlapQuery(
            session,
            NormalizedURL.from("s3://bucket/path"),
            SecurableType.EXTERNAL_LOCATION,
            /* limit= */ 10,
            /* includeParent= */ false,
            /* includeSelf= */ false,
            /* includeSubdir= */ true);

    validateQuery(
        query3,
        "FROM ExternalLocationDAO WHERE url LIKE :likePattern ESCAPE '\\' "
            + "ORDER BY LENGTH(url) DESC",
        null,
        "s3://bucket/path/%");

    // Test 4: includeParent + includeSelf
    Query<ExternalLocationDAO> query4 =
        ExternalLocationUtils.generateEntitiesDAOsWithURLOverlapQuery(
            session,
            NormalizedURL.from("s3://bucket/path"),
            SecurableType.EXTERNAL_LOCATION,
            /* limit= */ 10,
            /* includeParent= */ true,
            /* includeSelf= */ true,
            /* includeSubdir= */ false);

    validateQuery(
        query4,
        "FROM ExternalLocationDAO WHERE url IN (:matchPaths) ORDER BY LENGTH(url) DESC",
        List.of("s3://bucket", "s3://bucket/path"),
        null);

    // Test 5: includeSelf + includeSubdir
    Query<ExternalLocationDAO> query5 =
        ExternalLocationUtils.generateEntitiesDAOsWithURLOverlapQuery(
            session,
            NormalizedURL.from("s3://bucket/path"),
            SecurableType.EXTERNAL_LOCATION,
            /* limit= */ 10,
            /* includeParent= */ false,
            /* includeSelf= */ true,
            /* includeSubdir= */ true);

    validateQuery(
        query5,
        "FROM ExternalLocationDAO WHERE url IN (:matchPaths) OR url LIKE :likePattern "
            + "ESCAPE '\\' ORDER BY LENGTH(url) DESC",
        List.of("s3://bucket/path"),
        "s3://bucket/path/%");

    // Test 6: includeParent + includeSubdir
    Query<ExternalLocationDAO> query6 =
        ExternalLocationUtils.generateEntitiesDAOsWithURLOverlapQuery(
            session,
            NormalizedURL.from("s3://bucket/path/to/file"),
            SecurableType.EXTERNAL_LOCATION,
            /* limit= */ 10,
            /* includeParent= */ true,
            /* includeSelf= */ false,
            /* includeSubdir= */ true);

    validateQuery(
        query6,
        "FROM ExternalLocationDAO WHERE url IN (:matchPaths) OR url LIKE :likePattern "
            + "ESCAPE '\\' ORDER BY LENGTH(url) DESC",
        List.of("s3://bucket/path/to", "s3://bucket/path", "s3://bucket"),
        "s3://bucket/path/to/file/%");

    // Test 7: All three flags (includeParent + includeSelf + includeSubdir)
    Query<ExternalLocationDAO> query7 =
        ExternalLocationUtils.generateEntitiesDAOsWithURLOverlapQuery(
            session,
            NormalizedURL.from("s3://bucket/path/to/file"),
            SecurableType.EXTERNAL_LOCATION,
            /* limit= */ 10,
            /* includeParent= */ true,
            /* includeSelf= */ true,
            /* includeSubdir= */ true);

    validateQuery(
        query7,
        "FROM ExternalLocationDAO WHERE url IN (:matchPaths) OR url LIKE :likePattern "
            + "ESCAPE '\\' ORDER BY LENGTH(url) DESC",
        List.of(
            "s3://bucket/path/to", "s3://bucket/path", "s3://bucket", "s3://bucket/path/to/file"),
        "s3://bucket/path/to/file/%");

    // Test 8: Special characters in URL - verify proper escaping
    // Using URL-encoded characters: %25 for %, %5C for \, _ stays as-is. Un-encoded characters
    // will be rejected by NormalizedURL.
    Query<ExternalLocationDAO> query8 =
        ExternalLocationUtils.generateEntitiesDAOsWithURLOverlapQuery(
            session,
            NormalizedURL.from("s3://bucket/path%25test_file%5Cdata/"),
            SecurableType.EXTERNAL_LOCATION,
            /* limit= */ 10,
            /* includeParent= */ true,
            /* includeSelf= */ true,
            /* includeSubdir= */ true);

    validateQuery(
        query8,
        "FROM ExternalLocationDAO WHERE url IN (:matchPaths) OR url LIKE :likePattern ESCAPE '\\' "
            + "ORDER BY LENGTH(url) DESC",
        List.of("s3://bucket", "s3://bucket/path%25test_file%5Cdata"),
        "s3://bucket/path\\%25test\\_file\\%5Cdata/%");

    // Test 9: URL with trailing slash - should be normalized
    // The normalized URL will have trailing slashes removed
    Query<ExternalLocationDAO> query9 =
        ExternalLocationUtils.generateEntitiesDAOsWithURLOverlapQuery(
            session,
            NormalizedURL.from("s3://bucket/path/"),
            SecurableType.EXTERNAL_LOCATION,
            /* limit= */ 10,
            /* includeParent= */ false,
            /* includeSelf= */ true,
            /* includeSubdir= */ true);

    validateQuery(
        query9,
        "FROM ExternalLocationDAO WHERE url IN (:matchPaths) OR url LIKE :likePattern "
            + "ESCAPE '\\' ORDER BY LENGTH(url) DESC",
        List.of("s3://bucket/path"),
        "s3://bucket/path/%");

    // Test 10: URL from local path
    // The normalized URL will have file:/// prefix.
    Query<ExternalLocationDAO> query10 =
        ExternalLocationUtils.generateEntitiesDAOsWithURLOverlapQuery(
            session,
            NormalizedURL.from("/tmp/path/"),
            SecurableType.EXTERNAL_LOCATION,
            /* limit= */ 10,
            /* includeParent= */ true,
            /* includeSelf= */ true,
            /* includeSubdir= */ true);

    validateQuery(
        query10,
        "FROM ExternalLocationDAO WHERE url IN (:matchPaths) OR url LIKE :likePattern "
            + "ESCAPE '\\' ORDER BY LENGTH(url) DESC",
        List.of("file:///tmp", "file:///", "file:///tmp/path"),
        "file:///tmp/path/%");

    // Test 11: look for uncommitted staging tables
    Query<ExternalLocationDAO> query11 =
        ExternalLocationUtils.generateEntitiesDAOsWithURLOverlapQuery(
            session,
            NormalizedURL.from("/tmp/path"),
            ExternalLocationUtils.UNCOMMITTED_STAGING_TABLE_DAO_INFO,
            /* limit= */ 10,
            /* includeParent= */ false,
            /* includeSelf= */ false,
            /* includeSubdir= */ true);

    validateQuery(
        query11,
        "FROM StagingTableDAO WHERE stageCommitted=false AND "
            + "(stagingLocation LIKE :likePattern ESCAPE '\\') "
            + "ORDER BY LENGTH(stagingLocation) DESC",
        null,
        "file:///tmp/path/%");
  }

  @Test
  public void testManagedLocation() {
    NormalizedURL parentStorageLocation = NormalizedURL.from("file:///tmp/storage");

    // Test table path generation
    UUID tableId = UUID.randomUUID();
    NormalizedURL tablePathUri =
        ExternalLocationUtils.getManagedLocationForTable(parentStorageLocation, tableId);
    assertThat(tablePathUri.toString()).isEqualTo("file:///tmp/storage/tables/" + tableId);

    // Test volume path generation
    UUID volumeId = UUID.randomUUID();
    NormalizedURL volumePathUri =
        ExternalLocationUtils.getManagedLocationForVolume(parentStorageLocation, volumeId);
    assertThat(volumePathUri.toString()).isEqualTo("file:///tmp/storage/volumes/" + volumeId);

    // Test catalog path generation
    UUID catalogId = UUID.randomUUID();
    NormalizedURL catalogPathUri =
        ExternalLocationUtils.getManagedLocationForCatalog(parentStorageLocation, catalogId);
    assertThat(catalogPathUri.toString())
        .isEqualTo("file:///tmp/storage/__unitystorage/catalogs/" + catalogId);

    // Test schema path generation
    UUID schemaId = UUID.randomUUID();
    NormalizedURL schemaPathUri =
        ExternalLocationUtils.getManagedLocationForSchema(parentStorageLocation, schemaId);
    assertThat(schemaPathUri.toString())
        .isEqualTo("file:///tmp/storage/__unitystorage/schemas/" + schemaId);
  }
}
