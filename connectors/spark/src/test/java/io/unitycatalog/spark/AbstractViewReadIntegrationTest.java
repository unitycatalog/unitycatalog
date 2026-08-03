package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * End-to-end guarantee that a Unity Catalog view is resolvable and readable through a real Spark
 * session on every supported Spark version (the mock-based {@code UCViewProxySuite} / {@code
 * UCViewReadOnlySuite} don't drive Spark's analyzer, so they miss view-resolution bugs such as the
 * view schema mode). Subclasses supply the version-appropriate way to create the view.
 */
public abstract class AbstractViewReadIntegrationTest extends BaseSparkIntegrationTest {

  protected static final String VIEW_NAME = "spark_test_view";
  protected static final String VIEW_QUERY = "SELECT 1 AS c";
  protected static final String VIEW_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NAME + "." + VIEW_NAME;

  /** Creates VIEW_NAME (as VIEW_QUERY) the way the Spark version under test allows. */
  protected abstract void createView();

  protected void createSessionAndView() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    createView();
  }

  @Test
  public void testViewIsReadableThroughSpark() {
    createSessionAndView();
    assertThat(sql("SELECT * FROM %s", VIEW_FULL_NAME))
        .extracting(row -> row.getInt(0))
        .containsExactly(1);
  }
}
