package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for ExternalLocationUtils including both unit tests for static helper methods and
 * integration tests that verify query generation with a real Hibernate session.
 */
public class ExternalLocationUtilsTest {

  private static SessionFactory sessionFactory;
  private static Session session;

  @BeforeAll
  public static void setUp() {
    // Create minimal properties
    ServerProperties serverProperties = new ServerProperties(new Properties());
    // Create Hibernate configurator and session factory
    HibernateConfigurator hibernateConfigurator = new HibernateConfigurator(serverProperties);
    sessionFactory = hibernateConfigurator.getSessionFactory();
    session = sessionFactory.openSession();
  }

  @AfterAll
  public static void tearDown() {
    session.close();
    sessionFactory.close();
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
