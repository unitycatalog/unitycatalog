package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.persist.dao.DroppableIdentifiableDAO;
import io.unitycatalog.server.persist.dao.ModelVersionInfoDAO;
import io.unitycatalog.server.persist.dao.RegisteredModelInfoDAO;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.persist.model.PurgeState;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.junit.jupiter.api.Test;

abstract class AbstractLifecycleSchemaMigrationTest {
  private static final UUID SCHEMA_ID = UUID.fromString("00000000-0000-0000-0000-000000000001");
  private static final UUID MODEL_ID = UUID.fromString("00000000-0000-0000-0000-000000000002");
  private static final UUID TABLE_ID = UUID.fromString("00000000-0000-0000-0000-000000000003");
  private static final UUID VOLUME_ID = UUID.fromString("00000000-0000-0000-0000-000000000004");
  private static final UUID STAGING_ID = UUID.fromString("00000000-0000-0000-0000-000000000005");
  private static final UUID VERSION_ID = UUID.fromString("00000000-0000-0000-0000-000000000006");
  private static final Date LAST_CLEANUP_AT = new Date(1_700_000_000_000L);

  protected abstract Properties databaseProperties();

  @Test
  void migrationPreservesExistingRowsAndStagingCleanupState() {
    Properties properties = databaseProperties();
    properties.setProperty("hibernate.hbm2ddl.auto", "create");
    createLegacySchema(properties);

    properties.setProperty("hibernate.hbm2ddl.auto", "update");
    HibernateConfigurator configurator = new HibernateConfigurator(properties);
    try (Session session = configurator.getSessionFactory().openSession()) {
      assertActive(session.get(TableInfoDAO.class, TABLE_ID), "table");
      assertActive(session.get(VolumeInfoDAO.class, VOLUME_ID), "volume");
      assertActive(session.get(RegisteredModelInfoDAO.class, MODEL_ID), "model");

      ModelVersionInfoDAO version = session.get(ModelVersionInfoDAO.class, VERSION_ID);
      assertThat(version).isNotNull();
      assertThat(version.getDroppedAt()).isNull();
      assertThat(version.getPurgeState()).isEqualTo(PurgeState.ACTIVE.getValue());
      assertThat(version.getNumCleanupRetries()).isZero();
      assertThat(version.getLastCleanupAt()).isNull();

      StagingTableDAO staging = session.get(StagingTableDAO.class, STAGING_ID);
      assertThat(staging).isNotNull();
      assertThat(staging.getName()).isEqualTo("staging");
      assertThat(staging.getDroppedName()).isNull();
      assertThat(staging.getDroppedAt()).isNull();
      assertThat(staging.getPurgeState()).isEqualTo(PurgeState.RUNNING.getValue());
      assertThat(staging.getNumCleanupRetries()).isEqualTo((short) 3);
      assertThat(staging.getLastCleanupAt().getTime()).isEqualTo(LAST_CLEANUP_AT.getTime());

      assertLifecycleStateRoundTrip(session);
    } finally {
      configurator.getSessionFactory().close();
    }
  }

  private static void assertActive(DroppableIdentifiableDAO resource, String name) {
    assertThat(resource).isNotNull();
    assertThat(resource.getName()).isEqualTo(name);
    assertThat(resource.getDroppedName()).isNull();
    assertThat(resource.getDroppedAt()).isNull();
    assertThat(resource.getPurgeState()).isEqualTo(PurgeState.ACTIVE.getValue());
    assertThat(resource.getNumCleanupRetries()).isZero();
    assertThat(resource.getLastCleanupAt()).isNull();
  }

  private static void assertLifecycleStateRoundTrip(Session session) {
    Date droppedAt = new Date(1_710_000_000_000L);
    Date cleanupAt = new Date(1_720_000_000_000L);
    session.beginTransaction();
    TableInfoDAO table = session.get(TableInfoDAO.class, TABLE_ID);
    table.setDroppedName("table_before_drop");
    table.setDroppedAt(droppedAt);
    table.setPurgeState(PurgeState.RUNNING.getValue());
    table.setNumCleanupRetries((short) 4);
    table.setLastCleanupAt(cleanupAt);
    session.getTransaction().commit();

    session.clear();
    TableInfoDAO reloaded = session.get(TableInfoDAO.class, TABLE_ID);
    assertThat(reloaded.getDroppedName()).isEqualTo("table_before_drop");
    assertThat(reloaded.getDroppedAt().getTime()).isEqualTo(droppedAt.getTime());
    assertThat(reloaded.getPurgeState()).isEqualTo(PurgeState.RUNNING.getValue());
    assertThat(reloaded.getNumCleanupRetries()).isEqualTo((short) 4);
    assertThat(reloaded.getLastCleanupAt().getTime()).isEqualTo(cleanupAt.getTime());
  }

  private static void createLegacySchema(Properties properties) {
    Configuration configuration = new Configuration().setProperties(properties);
    configuration.addAnnotatedClass(LegacyTable.class);
    configuration.addAnnotatedClass(LegacyVolume.class);
    configuration.addAnnotatedClass(LegacyRegisteredModel.class);
    configuration.addAnnotatedClass(LegacyStagingTable.class);
    configuration.addAnnotatedClass(LegacyModelVersion.class);
    ServiceRegistry registry =
        new StandardServiceRegistryBuilder().applySettings(properties).build();

    try (SessionFactory sessionFactory = configuration.buildSessionFactory(registry);
        Session session = sessionFactory.openSession()) {
      session.beginTransaction();
      session.persist(new LegacyTable(TABLE_ID, "table", SCHEMA_ID));
      session.persist(new LegacyVolume(VOLUME_ID, "volume", SCHEMA_ID));
      session.persist(new LegacyRegisteredModel(MODEL_ID, "model", SCHEMA_ID));
      session.persist(new LegacyStagingTable(STAGING_ID, "staging", SCHEMA_ID));
      session.persist(new LegacyModelVersion(VERSION_ID, MODEL_ID));
      session.getTransaction().commit();
    }
  }

  @MappedSuperclass
  static class LegacyNamedResource {
    @Id
    @Column(name = "id")
    UUID id;

    @Column(name = "name", nullable = false)
    String name;

    @Column(name = "schema_id")
    UUID schemaId;

    LegacyNamedResource() {}

    LegacyNamedResource(UUID id, String name, UUID schemaId) {
      this.id = id;
      this.name = name;
      this.schemaId = schemaId;
    }
  }

  @Entity(name = "LegacyTable")
  @Table(name = "uc_tables")
  static class LegacyTable extends LegacyNamedResource {
    LegacyTable() {}

    LegacyTable(UUID id, String name, UUID schemaId) {
      super(id, name, schemaId);
    }
  }

  @Entity(name = "LegacyVolume")
  @Table(name = "uc_volumes")
  static class LegacyVolume extends LegacyNamedResource {
    LegacyVolume() {}

    LegacyVolume(UUID id, String name, UUID schemaId) {
      super(id, name, schemaId);
    }
  }

  @Entity(name = "LegacyRegisteredModel")
  @Table(name = "uc_registered_models")
  static class LegacyRegisteredModel extends LegacyNamedResource {
    LegacyRegisteredModel() {}

    LegacyRegisteredModel(UUID id, String name, UUID schemaId) {
      super(id, name, schemaId);
    }
  }

  @Entity(name = "LegacyStagingTable")
  @Table(name = "uc_staging_tables")
  static class LegacyStagingTable extends LegacyNamedResource {
    @Column(name = "staging_location", length = 2048, nullable = false)
    String stagingLocation = "file:/staging";

    @Column(name = "created_at", nullable = false)
    Date createdAt = LAST_CLEANUP_AT;

    @Column(name = "accessed_at", nullable = false)
    Date accessedAt = LAST_CLEANUP_AT;

    @Column(name = "stage_committed", nullable = false)
    boolean stageCommitted;

    @Column(name = "purge_state", nullable = false)
    short purgeState = PurgeState.RUNNING.getValue();

    @Column(name = "num_cleanup_retries", nullable = false)
    short numCleanupRetries = 3;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "last_cleanup_at")
    Date lastCleanupAt = LAST_CLEANUP_AT;

    LegacyStagingTable() {}

    LegacyStagingTable(UUID id, String name, UUID schemaId) {
      super(id, name, schemaId);
    }
  }

  @Entity(name = "LegacyModelVersion")
  @Table(name = "uc_model_versions")
  static class LegacyModelVersion {
    @Id
    @Column(name = "id")
    UUID id;

    @Column(name = "registered_model_id")
    UUID registeredModelId;

    LegacyModelVersion() {}

    LegacyModelVersion(UUID id, UUID registeredModelId) {
      this.id = id;
      this.registeredModelId = registeredModelId;
    }
  }
}
