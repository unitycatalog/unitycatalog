package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.KeyMapper;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.ColumnTypeName;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TableServiceMetricTest {

  private TableRepository tableRepository;
  private SchemaRepository schemaRepository;
  private SimpleMeterRegistry registry;
  private TableService service;

  @BeforeEach
  public void setUp() {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    Repositories repositories = mock(Repositories.class);
    tableRepository = mock(TableRepository.class);
    schemaRepository = mock(SchemaRepository.class);
    UserRepository userRepository = mock(UserRepository.class);
    KeyMapper keyMapper = mock(KeyMapper.class);
    ServerProperties serverProperties = mock(ServerProperties.class);

    when(repositories.getTableRepository()).thenReturn(tableRepository);
    when(repositories.getSchemaRepository()).thenReturn(schemaRepository);
    when(repositories.getUserRepository()).thenReturn(userRepository);
    when(repositories.getKeyMapper()).thenReturn(keyMapper);
    when(userRepository.findPrincipalId()).thenReturn(UUID.randomUUID());

    // The counter is registered in the constructor, so it reads 0 before any create.
    registry = new SimpleMeterRegistry();
    service = new TableService(authorizer, repositories, serverProperties, registry);
  }

  @Test
  public void createTableIncrementsCounter() {
    String tableId = UUID.randomUUID().toString();
    String schemaId = UUID.randomUUID().toString();
    when(tableRepository.createTable(any()))
        .thenReturn(new TableInfo().catalogName("c").schemaName("s").tableId(tableId));
    when(schemaRepository.getSchema("c.s")).thenReturn(new SchemaInfo().schemaId(schemaId));

    service.createTable(sampleCreateTable());

    assertThat(counterValue()).isEqualTo(1.0);
  }

  @Test
  public void failedCreateDoesNotIncrementCounter() {
    // createTable() calls the repository before touching the counter, so a create failure must
    // leave the counter untouched — we only count tables that were actually created.
    when(tableRepository.createTable(any())).thenThrow(new RuntimeException("create failed"));

    assertThatThrownBy(() -> service.createTable(sampleCreateTable()))
        .isInstanceOf(RuntimeException.class);

    assertThat(counterValue()).isEqualTo(0.0);
  }

  @Test
  public void createTableCountsEvenIfResponseBuildingFails() {
    // The table is persisted by tableRepository.createTable(); a failure in a later step (here,
    // resolving the schema for the response) must still count the table that was created.
    String tableId = UUID.randomUUID().toString();
    when(tableRepository.createTable(any()))
        .thenReturn(new TableInfo().catalogName("c").schemaName("s").tableId(tableId));
    when(schemaRepository.getSchema("c.s")).thenThrow(new RuntimeException("schema lookup failed"));

    assertThatThrownBy(() -> service.createTable(sampleCreateTable()))
        .isInstanceOf(RuntimeException.class);

    assertThat(counterValue()).isEqualTo(1.0);
  }

  private double counterValue() {
    return registry.get("uc.tables.created").counter().count();
  }

  private static CreateTable sampleCreateTable() {
    return new CreateTable()
        .name("t")
        .catalogName("c")
        .schemaName("s")
        .columns(
            List.of(
                new ColumnInfo()
                    .name("col1")
                    .typeName(ColumnTypeName.INT)
                    .typeText("INTEGER")
                    .position(0)
                    .nullable(true)))
        .tableType(TableType.EXTERNAL)
        .dataSourceFormat(DataSourceFormat.DELTA)
        .storageLocation("/tmp/t");
  }
}
