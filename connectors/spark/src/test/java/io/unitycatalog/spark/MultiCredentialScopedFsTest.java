package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.hadoop.internal.CredPropsUtil;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that one Spark query can read a UC-registered Parquet table whose files span two
 * credential-scoped paths in the same bucket.
 *
 * <pre>
 * s3://bucket/location-a/    credential A
 * ├── part-*.parquet
 * └── location-b/            credential B (more specific)
 *     └── part-*.parquet
 * </pre>
 */
public class MultiCredentialScopedFsTest extends BaseSparkIntegrationTest {

  private static final String LOCATION_A = "location-a";
  private static final String LOCATION_B = "location-b";
  private static final String TABLE_NAME = "multi_credential_scoped_fs";
  private static final String FULL_TABLE_NAME = CATALOG_NAME + "." + SCHEMA_NAME + "." + TABLE_NAME;
  private static final String QUERY = "SELECT * FROM %s ORDER BY i";

  @TempDir private Path dataDir;
  private String locationA;
  private String locationB;

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    session
        .sparkContext()
        .hadoopConfiguration()
        .set("fs.s3.impl", PerPathCredentialTestFileSystem.class.getName());
    locationA = "s3://bucket" + dataDir.resolve(LOCATION_A).normalize();
    locationB = locationA + "/" + LOCATION_B;
  }

  @AfterEach
  @Override
  public void cleanUp() {
    try {
      super.cleanUp();
    } finally {
      CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
      CredentialTestFileSystem.credentialCheckEnabled = true;
      PerPathCredentialTestFileSystem.clearExpectedCredentials();
      PerPathCredentialTestFileSystem.clearVendedCredentials();
    }
  }

  @Test
  public void bothCredentialsAreRequiredToReadBothLocations() {
    CredentialTestFileSystem.credentialCheckEnabled = false;
    try {
      sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS i, 'a' AS s", locationA);
      sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 2 AS i, 'b' AS s", locationB);
    } finally {
      CredentialTestFileSystem.credentialCheckEnabled = true;
    }

    PerPathCredentialTestFileSystem.setRequiredCredential(
        locationA, credential(LOCATION_A, locationA));
    PerPathCredentialTestFileSystem.setRequiredCredential(
        locationB, credential(LOCATION_B, locationB));

    vend(credential(LOCATION_A, locationA));

    sql(
        "CREATE TABLE %s (i INT, s STRING) USING PARQUET "
            + "OPTIONS (recursiveFileLookup 'true') LOCATION '%s'",
        FULL_TABLE_NAME, locationA);

    assertThatThrownBy(() -> sql(QUERY, FULL_TABLE_NAME))
        .rootCause()
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("expected access key ak-" + LOCATION_B + " for " + locationB);

    vend(credential(LOCATION_A, locationA), credential(LOCATION_B, locationB));

    assertThat(sql(QUERY, FULL_TABLE_NAME))
        .extracting(row -> row.getInt(0), row -> row.getString(1))
        .containsExactly(tuple(1, "a"), tuple(2, "b"));
  }

  private static AwsCredential credential(String id, String location) {
    return new AwsCredential("ak-" + id, "sk-" + id, "st-" + id, 1L, location);
  }

  private static void vend(GenericCredential... credentials) {
    List<GenericCredential> vendedCredentials = List.of(credentials);
    GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
    try {
      when(fetcher.createCredentials()).thenReturn(vendedCredentials);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    CredPropsUtil.genericCredFetcherFactory = (apiClient, credId) -> fetcher;
    PerPathCredentialTestFileSystem.setVendedCredentials(vendedCredentials);
  }
}
