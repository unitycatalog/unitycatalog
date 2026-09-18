package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;
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
import org.junit.jupiter.api.Test;

public class TableServiceMetricTest {

  @Test
  public void createTableIncrementsCounter() {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    Repositories repositories = mock(Repositories.class);
    TableRepository tableRepository = mock(TableRepository.class);
    SchemaRepository schemaRepository = mock(SchemaRepository.class);
    UserRepository userRepository = mock(UserRepository.class);
    KeyMapper keyMapper = mock(KeyMapper.class);
    ServerProperties serverProperties = mock(ServerProperties.class);

    when(repositories.getTableRepository()).thenReturn(tableRepository);
    when(repositories.getSchemaRepository()).thenReturn(schemaRepository);
    when(repositories.getUserRepository()).thenReturn(userRepository);
    when(repositories.getKeyMapper()).thenReturn(keyMapper);

    String tableId = UUID.randomUUID().toString();
    String schemaId = UUID.randomUUID().toString();
    when(tableRepository.createTable(any()))
        .thenReturn(new TableInfo().catalogName("c").schemaName("s").tableId(tableId));
    when(schemaRepository.getSchema("c.s")).thenReturn(new SchemaInfo().schemaId(schemaId));
    when(userRepository.findPrincipalId()).thenReturn(UUID.randomUUID());

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    TableService service = new TableService(authorizer, repositories, serverProperties, registry);

    CreateTable createTable =
        new CreateTable()
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

    service.createTable(createTable);

    assertThat(registry.get("uc.tables.created").counter().count()).isEqualTo(1.0);
  }
}
