package io.unitycatalog.server.service;

import static io.unitycatalog.server.service.credential.CredentialContext.READ_ONLY;
import static io.unitycatalog.server.service.credential.CredentialContext.READ_WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.StagingTableRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.service.iceberg.MetadataService;
import io.unitycatalog.server.service.iceberg.TableConfigService;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IcebergRestCatalogServiceTest {

  private final UnityCatalogAuthorizer authorizer = mock();
  private final Repositories repositories = mock();
  private final UserRepository userRepository = mock();
  private final ServerProperties serverProperties = mock();
  private final TableRepository tableRepository = mock();
  private final TableConfigService tableConfigService = mock();
  private IcebergRestCatalogService service;

  @BeforeEach
  public void setUp() {
    when(repositories.getUserRepository()).thenReturn(userRepository);
    when(repositories.getCatalogRepository()).thenReturn(mock(CatalogRepository.class));
    when(repositories.getSchemaRepository()).thenReturn(mock(SchemaRepository.class));
    when(repositories.getStagingTableRepository()).thenReturn(mock(StagingTableRepository.class));
    when(repositories.getTableRepository()).thenReturn(tableRepository);
    when(repositories.getSessionFactory()).thenReturn(mock(SessionFactory.class));
    service =
        new IcebergRestCatalogService(
            authorizer,
            tableConfigService,
            mock(MetadataService.class),
            repositories,
            serverProperties);
  }

  @Test
  public void nativeIcebergLoadFallsBackToReadOnlyWithoutWriteAccess() {
    UUID principalId = UUID.randomUUID();
    UUID tableId = UUID.randomUUID();
    when(userRepository.findPrincipalId()).thenReturn(principalId);
    when(authorizer.authorize(principalId, tableId, Privileges.OWNER)).thenReturn(false);
    when(authorizer.authorizeAll(principalId, tableId, Privileges.SELECT, Privileges.MODIFY))
        .thenReturn(false);

    assertThat(
            service.getLoadCredentialPrivileges(
                new TableRepository.IcebergTableState(
                    tableId,
                    DataSourceFormat.ICEBERG,
                    "s3://bucket/table/metadata.json",
                    "s3://bucket/table")))
        .isEqualTo(READ_ONLY);
  }

  @Test
  public void nativeIcebergLoadUsesReadWriteWithOwnerAccess() {
    UUID principalId = UUID.randomUUID();
    UUID tableId = UUID.randomUUID();
    when(userRepository.findPrincipalId()).thenReturn(principalId);
    when(authorizer.authorize(principalId, tableId, Privileges.OWNER)).thenReturn(true);

    assertThat(
            service.getLoadCredentialPrivileges(
                new TableRepository.IcebergTableState(
                    tableId,
                    DataSourceFormat.ICEBERG,
                    "s3://bucket/table/metadata.json",
                    "s3://bucket/table")))
        .isEqualTo(READ_WRITE);
  }

  @Test
  public void uniformLoadAlwaysUsesReadOnlyCredentials() {
    assertThat(
            service.getLoadCredentialPrivileges(
                new TableRepository.IcebergTableState(
                    UUID.randomUUID(),
                    DataSourceFormat.DELTA,
                    "s3://bucket/table/metadata.json",
                    "s3://bucket/table")))
        .isEqualTo(READ_ONLY);
  }

  @Test
  public void configAdvertisesWritesOnlyWhenIcebergTableWritesAreEnabled() {
    when(serverProperties.isIcebergTableEnabled()).thenReturn(false);
    ConfigResponse readOnly = service.config(java.util.Optional.of("catalog"));
    assertThat(readOnly.endpoints()).doesNotContain(Endpoint.V1_CREATE_TABLE);
    assertThat(readOnly.endpoints()).doesNotContain(Endpoint.V1_UPDATE_TABLE);

    when(serverProperties.isIcebergTableEnabled()).thenReturn(true);
    ConfigResponse writable = service.config(java.util.Optional.of("catalog"));
    assertThat(writable.endpoints()).contains(Endpoint.V1_CREATE_TABLE, Endpoint.V1_UPDATE_TABLE);
  }

  @Test
  public void configAdvertisesLoadCredentials() {
    // A client only looks for a refresh endpoint if the catalog says it serves this one.
    when(serverProperties.isIcebergTableEnabled()).thenReturn(false);
    assertThat(service.config(java.util.Optional.of("catalog")).endpoints())
        .contains(Endpoint.V1_TABLE_CREDENTIALS);
  }

  @Test
  public void loadCredentialsVendsWithTheSamePrivilegesAsLoadTable() {
    UUID principalId = UUID.randomUUID();
    UUID tableId = UUID.randomUUID();
    when(userRepository.findPrincipalId()).thenReturn(principalId);
    when(authorizer.authorize(principalId, tableId, Privileges.OWNER)).thenReturn(true);
    when(tableRepository.getIcebergTableState("cat", "ns", "tbl"))
        .thenReturn(
            new TableRepository.IcebergTableState(
                tableId,
                DataSourceFormat.ICEBERG,
                "s3://bucket/table/metadata.json",
                "s3://bucket/table"));
    when(tableConfigService.getTableConfig(any(), any(), any()))
        .thenReturn(Map.of(S3FileIOProperties.ACCESS_KEY_ID, "AKIA"));

    LoadCredentialsResponse response = service.loadCredentials("cat", "ns", "tbl");

    assertThat(response.credentials())
        .singleElement()
        .satisfies(
            credential -> {
              assertThat(credential.prefix()).isEqualTo("s3://bucket/table");
              assertThat(credential.config())
                  .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "AKIA");
            });
    verify(tableConfigService)
        .getTableConfig(
            NormalizedURL.from("s3://bucket/table"),
            READ_WRITE,
            "v1/catalogs/cat/namespaces/ns/tables/tbl/credentials");
  }

  @Test
  public void loadCredentialsFallsBackToReadOnlyWithoutWriteAccess() {
    UUID principalId = UUID.randomUUID();
    UUID tableId = UUID.randomUUID();
    when(userRepository.findPrincipalId()).thenReturn(principalId);
    when(authorizer.authorize(principalId, tableId, Privileges.OWNER)).thenReturn(false);
    when(authorizer.authorizeAll(principalId, tableId, Privileges.SELECT, Privileges.MODIFY))
        .thenReturn(false);
    when(tableRepository.getIcebergTableState("cat", "ns", "tbl"))
        .thenReturn(
            new TableRepository.IcebergTableState(
                tableId,
                DataSourceFormat.ICEBERG,
                "s3://bucket/table/metadata.json",
                "s3://bucket/table"));
    when(tableConfigService.getTableConfig(any(), any(), any()))
        .thenReturn(Map.of(S3FileIOProperties.ACCESS_KEY_ID, "AKIA"));

    service.loadCredentials("cat", "ns", "tbl");

    verify(tableConfigService).getTableConfig(any(), eq(READ_ONLY), any());
  }

  @Test
  public void loadCredentialsVendsNothingForATableThatNeedsNoCredentials() {
    // A local (file://) table vends an empty config, which Iceberg's Credential type rejects.
    when(tableRepository.getIcebergTableState("cat", "ns", "tbl"))
        .thenReturn(
            new TableRepository.IcebergTableState(
                UUID.randomUUID(),
                DataSourceFormat.DELTA,
                "file:///tmp/table/metadata.json",
                "file:///tmp/table"));
    when(tableConfigService.getTableConfig(any(), any(), any())).thenReturn(Map.of());

    assertThat(service.loadCredentials("cat", "ns", "tbl").credentials()).isEmpty();
  }

  @Test
  public void loadCredentialsForAMissingTableIsNotFound() {
    when(tableRepository.getIcebergTableState("cat", "ns", "tbl"))
        .thenReturn(
            new TableRepository.IcebergTableState(
                UUID.randomUUID(), DataSourceFormat.ICEBERG, null, null));

    assertThatThrownBy(() -> service.loadCredentials("cat", "ns", "tbl"))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("ns.tbl");
  }
}
