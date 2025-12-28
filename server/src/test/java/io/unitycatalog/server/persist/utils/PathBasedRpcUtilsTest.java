package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import java.util.Properties;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for PathBasedRpcUtils including both unit tests for static helper methods and integration
 * tests that verify query generation with a real Hibernate session.
 */
public class PathBasedRpcUtilsTest {

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

  @Test
  public void testGetParentAndCurrentPathsList() {
    // Test S3 nested path
    List<String> s3Result = PathBasedRpcUtils.getParentPathsList("s3://bucket/path/to/file");
    String[] expectedResultForPathToFile =
        new String[] {"s3://bucket/path/to", "s3://bucket/path", "s3://bucket"};
    assertThat(s3Result).containsExactly(expectedResultForPathToFile);

    // Test S3 path with trailing slash (should normalize)
    List<String> s3TrailingResult =
        PathBasedRpcUtils.getParentPathsList("s3://bucket/path/to/file/");
    assertThat(s3TrailingResult).containsExactly(expectedResultForPathToFile);

    // Test S3 bucket only (no path)
    List<String> s3BucketResult = PathBasedRpcUtils.getParentPathsList("s3://bucket");
    assertThat(s3BucketResult).isEmpty();

    List<String> s3BucketRootResult = PathBasedRpcUtils.getParentPathsList("s3://bucket/");
    assertThat(s3BucketRootResult).isEmpty();

    // Test S3 single path level
    List<String> s3SingleLevelResult = PathBasedRpcUtils.getParentPathsList("s3://bucket/path");
    assertThat(s3SingleLevelResult).containsExactly("s3://bucket");

    // Test Azure Blob Storage path
    List<String> azureResult =
        PathBasedRpcUtils.getParentPathsList(
            "abfs://container@storage.dfs.core.windows.net/path/to/file");
    assertThat(azureResult)
        .containsExactly(
            "abfs://container@storage.dfs.core.windows.net/path/to",
            "abfs://container@storage.dfs.core.windows.net/path",
            "abfs://container@storage.dfs.core.windows.net");

    // Test Google Cloud Storage path
    List<String> gcsResult = PathBasedRpcUtils.getParentPathsList("gs://bucket/path/to/file");
    assertThat(gcsResult).containsExactly("gs://bucket/path/to", "gs://bucket/path", "gs://bucket");

    // Test local file path
    assertThat(PathBasedRpcUtils.getParentPathsList("file:/tmp/path/to/file"))
        .containsExactly("file:///tmp/path/to", "file:///tmp/path", "file:///tmp", "file:///");
    assertThat(PathBasedRpcUtils.getParentPathsList("file:///tmp/")).containsExactly("file:///");
    assertThat(PathBasedRpcUtils.getParentPathsList("file:///tmp")).containsExactly("file:///");

    // Test deeply nested path (10 levels)
    List<String> deepResult =
        PathBasedRpcUtils.getParentPathsList("s3://bucket/a/b/c/d/e/f/g/h/i/j");
    assertThat(deepResult)
        .containsExactly(
            "s3://bucket/a/b/c/d/e/f/g/h/i",
            "s3://bucket/a/b/c/d/e/f/g/h",
            "s3://bucket/a/b/c/d/e/f/g",
            "s3://bucket/a/b/c/d/e/f",
            "s3://bucket/a/b/c/d/e",
            "s3://bucket/a/b/c/d",
            "s3://bucket/a/b/c",
            "s3://bucket/a/b",
            "s3://bucket/a",
            "s3://bucket");
  }

  @Test
  public void testEscapeLikePattern() {
    // Test string with no special characters
    String noSpecialResult = PathBasedRpcUtils.escapeLikePattern("s3://bucket/path");
    assertThat(noSpecialResult).isEqualTo("s3://bucket/path");

    // Test string with percent sign
    String percentResult = PathBasedRpcUtils.escapeLikePattern("s3://bucket/path%test");
    assertThat(percentResult).isEqualTo("s3://bucket/path\\%test");

    // Test string with underscore
    String underscoreResult = PathBasedRpcUtils.escapeLikePattern("s3://bucket/path_test");
    assertThat(underscoreResult).isEqualTo("s3://bucket/path\\_test");

    // Test string with backslash
    String backslashResult = PathBasedRpcUtils.escapeLikePattern("s3://bucket/path\\test");
    assertThat(backslashResult).isEqualTo("s3://bucket/path\\\\test");

    // Test string with all special characters
    String allSpecialResult = PathBasedRpcUtils.escapeLikePattern("s3://bucket/a%b_c\\d");
    assertThat(allSpecialResult).isEqualTo("s3://bucket/a\\%b\\_c\\\\d");

    // Test empty string
    String emptyResult = PathBasedRpcUtils.escapeLikePattern("");
    assertThat(emptyResult).isEqualTo("");
  }

  @Test
  public void testUnsupportedSecurableType() {
    // METASTORE, CATALOG, SCHEMA, FUNCTION are not supported for URL overlap checks
    assertThatThrownBy(
            () ->
                PathBasedRpcUtils.getEntitiesDAOsOverlapUrl(
                    null,
                    "s3://bucket/path",
                    SecurableType.METASTORE,
                    1,
                    /* includeSelf= */ true,
                    /* includeParent= */ true,
                    /* includeSubdir= */ true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported securable type for URL overlap check");

    assertThatThrownBy(
            () ->
                PathBasedRpcUtils.getEntitiesDAOsOverlapUrl(
                    null,
                    "s3://bucket/path",
                    SecurableType.CATALOG,
                    1,
                    /* includeSelf= */ true,
                    /* includeParent= */ true,
                    /* includeSubdir= */ true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported securable type for URL overlap check");

    assertThatThrownBy(
            () ->
                PathBasedRpcUtils.getEntitiesDAOsOverlapUrl(
                    null,
                    "s3://bucket/path",
                    SecurableType.SCHEMA,
                    1,
                    /* includeSelf= */ true,
                    /* includeParent= */ true,
                    /* includeSubdir= */ true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported securable type for URL overlap check");
  }

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
  public void testGenerateEntitiesDAOsOverlapUrlQueryWithRealSession() {
    // Test 1: includeSelf only
    Query<ExternalLocationDAO> query1 =
        PathBasedRpcUtils.generateEntitiesDAOsOverlapUrlQuery(
            session,
            "s3://bucket/path",
            ExternalLocationDAO.class,
            "url",
            10,
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
        PathBasedRpcUtils.generateEntitiesDAOsOverlapUrlQuery(
            session,
            "s3://bucket/path/to/file",
            ExternalLocationDAO.class,
            "url",
            10,
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
        PathBasedRpcUtils.generateEntitiesDAOsOverlapUrlQuery(
            session,
            "s3://bucket/path",
            ExternalLocationDAO.class,
            "url",
            10,
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
        PathBasedRpcUtils.generateEntitiesDAOsOverlapUrlQuery(
            session,
            "s3://bucket/path",
            ExternalLocationDAO.class,
            "url",
            10,
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
        PathBasedRpcUtils.generateEntitiesDAOsOverlapUrlQuery(
            session,
            "s3://bucket/path",
            ExternalLocationDAO.class,
            "url",
            10,
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
        PathBasedRpcUtils.generateEntitiesDAOsOverlapUrlQuery(
            session,
            "s3://bucket/path/to/file",
            ExternalLocationDAO.class,
            "url",
            10,
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
        PathBasedRpcUtils.generateEntitiesDAOsOverlapUrlQuery(
            session,
            "s3://bucket/path/to/file",
            ExternalLocationDAO.class,
            "url",
            10,
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
    Query<ExternalLocationDAO> query8 =
        PathBasedRpcUtils.generateEntitiesDAOsOverlapUrlQuery(
            session,
            "s3://bucket/path%with_special\\chars",
            ExternalLocationDAO.class,
            "url",
            10,
            /* includeParent= */ false,
            /* includeSelf= */ false,
            /* includeSubdir= */ true);

    validateQuery(
        query8,
        "FROM ExternalLocationDAO WHERE url LIKE :likePattern ESCAPE '\\' "
            + "ORDER BY LENGTH(url) DESC",
        null,
        "s3://bucket/path\\%with\\_special\\\\chars/%");

    // Test 9: URL with trailing slash - should be normalized
    Query<ExternalLocationDAO> query9 =
        PathBasedRpcUtils.generateEntitiesDAOsOverlapUrlQuery(
            session,
            "s3://bucket/path/",
            ExternalLocationDAO.class,
            "url",
            10,
            /* includeParent= */ false,
            /* includeSelf= */ true,
            /* includeSubdir= */ true);

    validateQuery(
        query9,
        "FROM ExternalLocationDAO WHERE url IN (:matchPaths) OR url LIKE :likePattern "
            + "ESCAPE '\\' ORDER BY LENGTH(url) DESC",
        List.of("s3://bucket/path/"),
        "s3://bucket/path/%");
  }
}
