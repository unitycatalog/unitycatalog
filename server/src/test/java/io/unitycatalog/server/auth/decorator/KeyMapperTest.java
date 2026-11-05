package io.unitycatalog.server.auth.decorator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests how {@link KeyMapper} merges ids derived from an EXTERNAL_LOCATION path with ids resolved
 * from explicit keys. A keyed CATALOG/SCHEMA/TABLE (the resource the request acts on) is
 * authoritative, so the caller-controlled path may only add ids the keys did not resolve, never
 * replace one, and a keyed-but-null id fails closed rather than adopting the path's owner. This
 * keeps the resolver's decision correct on its own; the create policies also guard a mismatched
 * location via {@code #no_overlap_with_data_securable} (external) and the staging-ownership check
 * (managed).
 */
public class KeyMapperTest {

  private SessionFactory sessionFactory;
  private Repositories repositories;

  private final UUID catalogId = UUID.randomUUID();
  private final UUID schemaAId = UUID.randomUUID();
  private final UUID schemaBId = UUID.randomUUID();
  private final UUID tableBId = UUID.randomUUID();

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    ServerProperties serverProperties = new ServerProperties(properties);
    Properties hibernateProperties =
        HibernateConfigurator.setupHibernateProperties(serverProperties);
    hibernateProperties.setProperty(
        "hibernate.connection.url", "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    sessionFactory = new HibernateConfigurator(hibernateProperties).getSessionFactory();
    repositories = new Repositories(sessionFactory, serverProperties);

    // One catalog with two schemas; an external table lives in schema B at s3://bucket/tbl_b.
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          session.persist(
              CatalogInfoDAO.builder().id(catalogId).name("cat").createdAt(new Date()).build());
          session.persist(
              SchemaInfoDAO.builder()
                  .id(schemaAId)
                  .catalogId(catalogId)
                  .name("sch_a")
                  .createdAt(new Date())
                  .build());
          session.persist(
              SchemaInfoDAO.builder()
                  .id(schemaBId)
                  .catalogId(catalogId)
                  .name("sch_b")
                  .createdAt(new Date())
                  .build());
          session.persist(
              TableInfoDAO.builder()
                  .id(tableBId)
                  .schemaId(schemaBId)
                  .name("tbl_b")
                  .type(TableType.EXTERNAL.getValue())
                  .dataSourceFormat(DataSourceFormat.DELTA.getValue())
                  .url("s3://bucket/tbl_b")
                  .createdAt(new Date())
                  .build());
          return null;
        },
        "Failed to seed test entities",
        /* readOnly= */ false);
  }

  @AfterEach
  void tearDown() {
    sessionFactory.close();
  }

  @Test
  void pathDerivedIdsDoNotOverwriteExplicitKeys() {
    // Catalog + schema A are keyed explicitly (as a URL would); the location lives under tbl_b,
    // which belongs to schema B.
    Map<SecurableType, Object> keys = new HashMap<>();
    keys.put(SecurableType.CATALOG, "cat");
    keys.put(SecurableType.SCHEMA, "sch_a");
    keys.put(SecurableType.EXTERNAL_LOCATION, "s3://bucket/tbl_b/data");

    Map<SecurableType, UUID> ids = repositories.getKeyMapper().mapResourceKeys(keys);

    // The keyed catalog/schema win; the path does not redirect them to schema B.
    assertThat(ids).containsEntry(SecurableType.CATALOG, catalogId);
    assertThat(ids).containsEntry(SecurableType.SCHEMA, schemaAId);
    // The path's owning table is still surfaced (so #no_overlap_with_data_securable sees it); it
    // just cannot clobber the keyed ids.
    assertThat(ids).containsEntry(SecurableType.TABLE, tableBId);
  }

  @Test
  void nullKeyedCatalogAndSchemaFailClosedInsteadOfAdoptingThePathOwner() {
    // A malformed create can omit catalog_name/schema_name; the decorator still keys them (as
    // null). The keyed-but-null catalog/schema force a failing lookup before the location branch
    // runs, so a location inside tbl_b never backfills tbl_b's catalog/schema as the authorization
    // target. Absent (rather than null) keys would be backfilled, which is why the decorator always
    // keys them.
    Map<SecurableType, Object> keys = new HashMap<>();
    keys.put(SecurableType.CATALOG, null);
    keys.put(SecurableType.SCHEMA, null);
    keys.put(SecurableType.EXTERNAL_LOCATION, "s3://bucket/tbl_b/data");

    // Fails closed on the unresolvable keyed catalog, not by adopting tbl_b's catalog/schema.
    assertThatThrownBy(() -> repositories.getKeyMapper().mapResourceKeys(keys))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.CATALOG_NOT_FOUND);
  }

  @Test
  void externalLocationPathStillResolvesWhenItAddsANewKey() {
    UUID externalLocationId = UUID.randomUUID();
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          session.persist(
              ExternalLocationDAO.builder()
                  .id(externalLocationId)
                  .name("ext")
                  .url("s3://bucket/ext")
                  .credentialId(UUID.randomUUID())
                  .build());
          return null;
        },
        "Failed to seed external location",
        /* readOnly= */ false);

    Map<SecurableType, Object> keys = new HashMap<>();
    keys.put(SecurableType.CATALOG, "cat");
    keys.put(SecurableType.SCHEMA, "sch_a");
    keys.put(SecurableType.EXTERNAL_LOCATION, "s3://bucket/ext/data");

    Map<SecurableType, UUID> ids = repositories.getKeyMapper().mapResourceKeys(keys);

    // A path under a plain external location (not overlapping a keyed resource) still resolves
    // #external_location, and the keyed catalog/schema are untouched.
    assertThat(ids).containsEntry(SecurableType.CATALOG, catalogId);
    assertThat(ids).containsEntry(SecurableType.SCHEMA, schemaAId);
    assertThat(ids).containsEntry(SecurableType.EXTERNAL_LOCATION, externalLocationId);
  }
}
