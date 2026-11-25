package io.unitycatalog.server.service;

import static io.unitycatalog.server.service.credential.CredentialContext.READ_ONLY;
import static io.unitycatalog.server.service.credential.CredentialContext.READ_WRITE;
import static io.unitycatalog.server.utils.IcebergViewTestUtils.request;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.auth.AuthorizeExpressions;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.KeyMapper;
import io.unitycatalog.server.auth.decorator.ResultFilter;
import io.unitycatalog.server.auth.decorator.UnityAccessDecorator;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.StagingTableRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.utils.FileOperationsImpl;
import io.unitycatalog.server.service.iceberg.MetadataService;
import io.unitycatalog.server.service.iceberg.TableConfigService;
import io.unitycatalog.server.service.iceberg.ViewMetadataService;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.types.Types;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

public class IcebergRestCatalogServiceTest {

  private static final String TABLE_LOCATION = "s3://bucket/table";

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  private final UnityCatalogAuthorizer authorizer = mock();
  private final Repositories repositories = mock();
  private final KeyMapper keyMapper = mock();
  private final UserRepository userRepository = mock();
  private final ServerProperties serverProperties = mock();
  private final TableRepository tableRepository = mock();
  private final TableConfigService tableConfigService = mock();
  private final MetadataService metadataService = mock();
  private IcebergRestCatalogService service;

  @BeforeEach
  public void setUp() {
    var fileOperations = new FileOperationsImpl(mock(), serverProperties);
    when(repositories.getFileOperations()).thenReturn(fileOperations);
    when(repositories.getUserRepository()).thenReturn(userRepository);
    when(repositories.getKeyMapper()).thenReturn(keyMapper);
    when(repositories.getCatalogRepository()).thenReturn(mock(CatalogRepository.class));
    when(repositories.getSchemaRepository()).thenReturn(mock(SchemaRepository.class));
    when(repositories.getStagingTableRepository()).thenReturn(mock(StagingTableRepository.class));
    when(repositories.getTableRepository()).thenReturn(tableRepository);
    when(repositories.getSessionFactory()).thenReturn(mock(SessionFactory.class));
    service = newService();
  }

  private IcebergRestCatalogService newService() {
    return new IcebergRestCatalogService(
        authorizer, tableConfigService, metadataService, repositories, serverProperties);
  }

  @Test
  public void viewReadsRejectASnapshotWithADifferentId(@TempDir Path root) {
    var views = authorizedViewService(root);
    UUID authorizedId = UUID.randomUUID();
    UUID snapshotId = UUID.randomUUID();
    when(tableRepository.getIcebergViewState("cat", "ns", "v"))
        .thenReturn(
            new TableRepository.IcebergViewState(
                snapshotId, root.resolve("view.json").toUri().toString()));
    var context = ServiceRequestContext.of(HttpRequest.of(HttpMethod.GET, "/"));
    context.setAttr(
        UnityAccessDecorator.RESULT_FILTER_ATTR,
        new ResultFilter(
            mock(UnityAccessEvaluator.class),
            UUID.randomUUID(),
            AuthorizeExpressions.GET_TABLE,
            "",
            Map.of(SecurableType.TABLE, authorizedId),
            Map.of(),
            keyMapper));
    try (var ignored = context.push()) {
      assertThat(
              assertThrows(BaseException.class, () -> views.loadView("cat", "ns", "v"))
                  .getErrorCode())
          .isEqualTo(ErrorCode.PERMISSION_DENIED);
      assertThat(
              assertThrows(BaseException.class, () -> views.viewExists("cat", "ns", "v"))
                  .getErrorCode())
          .isEqualTo(ErrorCode.PERMISSION_DENIED);
    }
  }

  private IcebergRestCatalogService authorizedViewService(Path root) {
    when(serverProperties.isIcebergViewEnabled()).thenReturn(true);
    when(serverProperties.isAuthorizationEnabled()).thenReturn(true);
    when(serverProperties.get(ServerProperties.Property.ICEBERG_VIEW_METADATA_ROOT))
        .thenReturn(root.toString());
    return newService();
  }

  @Test
  public void createRequiresTheAuthorizedSchemaBeforeWritingMetadata(@TempDir Path root) {
    var views = authorizedViewService(root);
    UUID authorizedId = UUID.randomUUID();
    when(repositories.getSchemaRepository().getSchemaIdOrThrow("cat", "ns"))
        .thenReturn(UUID.randomUUID());
    var context = ServiceRequestContext.of(HttpRequest.of(HttpMethod.POST, "/"));
    context.setAttr(
        UnityAccessDecorator.RESULT_FILTER_ATTR,
        new ResultFilter(
            mock(UnityAccessEvaluator.class),
            UUID.randomUUID(),
            AuthorizeExpressions.CREATE_TABLE,
            "",
            Map.of(SecurableType.SCHEMA, authorizedId),
            Map.of(),
            keyMapper));
    try (var ignored = context.push()) {
      var error =
          assertThrows(
              BaseException.class,
              () -> views.createView("cat", "ns", request("v", "spark", "select 1 as id")));
      assertThat(error.getErrorCode()).isEqualTo(ErrorCode.PERMISSION_DENIED);
    }
    verifyNoInteractions(tableRepository);
    assertThat(root).isEmptyDirectory();
  }

  @Test
  public void rejectedDropPassesTheAuthorizedIdAndKeepsGrants(@TempDir Path root) {
    var views = authorizedViewService(root);
    UUID viewId = UUID.randomUUID();
    TableInfo view = new TableInfo().tableId(viewId.toString());
    String pointer = root.resolve("view.json").toUri().toString();
    when(tableRepository.getIcebergViewState("cat", "ns", "v"))
        .thenReturn(new TableRepository.IcebergViewState(viewId, pointer));
    var filter = mock(ResultFilter.class);
    when(filter.filterResult(SecurableType.TABLE, view, null)).thenReturn(view);
    var context = ServiceRequestContext.of(HttpRequest.of(HttpMethod.DELETE, "/"));
    context.setAttr(UnityAccessDecorator.RESULT_FILTER_ATTR, filter);
    var denied = new BaseException(ErrorCode.PERMISSION_DENIED, "Access denied.");
    when(tableRepository.deleteViewForIceberg("cat", "ns", "v", viewId)).thenThrow(denied);
    try (var ignored = context.push()) {
      assertThatThrownBy(() -> views.dropView("cat", "ns", "v")).isSameAs(denied);
    }
    verify(filter).filterResult(SecurableType.TABLE, view, null);
    verifyNoInteractions(authorizer);
  }

  @ParameterizedTest
  @EnumSource(
      value = ErrorCode.class,
      names = {
        "TABLE_ALREADY_EXISTS",
        "CATALOG_NOT_FOUND",
        "SCHEMA_NOT_FOUND",
        "INVALID_ARGUMENT",
        "COMMIT_STATE_UNKNOWN"
      })
  public void viewMetadataCleanupDependsOnTheRegistrationOutcome(
      ErrorCode code, @TempDir Path root) {
    when(serverProperties.isIcebergViewEnabled()).thenReturn(true);
    when(serverProperties.get(ServerProperties.Property.ICEBERG_VIEW_METADATA_ROOT))
        .thenReturn(root.toString());
    var views = newService();
    UUID schemaId = UUID.randomUUID();
    when(repositories.getSchemaRepository().getSchemaIdOrThrow("cat", "ns")).thenReturn(schemaId);
    var failure = new BaseException(code, "Registration failed");
    when(tableRepository.createViewForIceberg(any(), eq(schemaId), any(), any()))
        .thenThrow(failure);

    assertThatThrownBy(() -> views.createView("cat", "ns", request("v", "spark", "select 1 as id")))
        .isSameAs(failure);

    var viewId = ArgumentCaptor.forClass(UUID.class);
    var pointer = ArgumentCaptor.forClass(String.class);
    verify(tableRepository)
        .createViewForIceberg(any(), eq(schemaId), viewId.capture(), pointer.capture());
    if (code == ErrorCode.COMMIT_STATE_UNKNOWN) {
      String expectedId = viewId.getValue().toString();
      var metadata =
          new ViewMetadataService(root.toString(), repositories.getFileOperations())
              .read(expectedId, pointer.getValue());
      assertThat(metadata.uuid()).isEqualTo(expectedId);
    } else {
      assertThat(root).isEmptyDirectory();
    }
  }

  @Test
  public void nativeIcebergLoadFallsBackToReadOnlyWithoutWriteAccess() {
    UUID principalId = UUID.randomUUID();
    UUID tableId = UUID.randomUUID();
    when(userRepository.findPrincipalId()).thenReturn(principalId);
    when(authorizer.authorize(principalId, SecurableType.TABLE, tableId, Privileges.OWNER))
        .thenReturn(false);
    when(authorizer.authorizeAll(
            principalId, SecurableType.TABLE, tableId, Privileges.SELECT, Privileges.MODIFY))
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
    when(authorizer.authorize(principalId, SecurableType.TABLE, tableId, Privileges.OWNER))
        .thenReturn(true);

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
    when(authorizer.authorize(principalId, SecurableType.TABLE, tableId, Privileges.OWNER))
        .thenReturn(true);
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
    when(authorizer.authorize(principalId, SecurableType.TABLE, tableId, Privileges.OWNER))
        .thenReturn(false);
    when(authorizer.authorizeAll(
            principalId, SecurableType.TABLE, tableId, Privileges.SELECT, Privileges.MODIFY))
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
  public void loadCredentialsRejectsDeniedOrUnavailableReadAccess() {
    UUID tableId = UUID.randomUUID();
    UUID principalId = UUID.randomUUID();
    UUID catalogId = UUID.randomUUID();
    UUID schemaId = UUID.randomUUID();
    when(userRepository.findPrincipalId()).thenReturn(principalId);
    when(serverProperties.isAuthorizationEnabled()).thenReturn(true);
    when(serverProperties.isStrictPrivilegeChecksEnabled()).thenReturn(true);
    when(tableRepository.getIcebergTableState("cat", "ns", "tbl"))
        .thenReturn(
            new TableRepository.IcebergTableState(
                tableId, DataSourceFormat.ICEBERG, "s3://bucket/metadata.json", "s3://bucket"));
    when(keyMapper.mapResourceKeys(
            Map.of(SecurableType.TABLE, tableId.toString(), SecurableType.METASTORE, "")))
        .thenReturn(
            Map.of(
                SecurableType.CATALOG, catalogId,
                SecurableType.SCHEMA, schemaId,
                SecurableType.METASTORE, UUID.randomUUID(),
                SecurableType.TABLE, tableId));
    when(authorizer.authorizeAny(
            principalId,
            SecurableType.CATALOG,
            catalogId,
            Privileges.OWNER,
            Privileges.USE_CATALOG))
        .thenReturn(true);
    when(authorizer.authorizeAny(
            principalId, SecurableType.SCHEMA, schemaId, Privileges.OWNER, Privileges.USE_SCHEMA))
        .thenReturn(true);

    assertThatThrownBy(() -> service.loadCredentials("cat", "ns", "tbl"))
        .isInstanceOfSatisfying(
            BaseException.class,
            error -> assertThat(error.getErrorCode()).isEqualTo(ErrorCode.PERMISSION_DENIED));
    verify(authorizer)
        .authorizeAny(
            principalId, SecurableType.TABLE, tableId, Privileges.OWNER, Privileges.SELECT);

    BaseException unavailable =
        new BaseException(ErrorCode.UNAVAILABLE, "Authorization service is unavailable");
    when(authorizer.authorizeAny(any(), any(), any(), any(Privileges[].class)))
        .thenThrow(unavailable);
    assertThatThrownBy(() -> service.loadCredentials("cat", "ns", "tbl")).isSameAs(unavailable);

    verifyNoInteractions(tableConfigService);
    verify(authorizer, never()).refreshAuthorizations(anyLong());
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

  /** Failures a table commit raises before or within a transaction that is rolled back. */
  static Stream<Arguments> rejectedCommitFailures() {
    return Stream.of(
            ErrorCode.UPDATE_REQUIREMENT_CONFLICT,
            ErrorCode.TABLE_NOT_FOUND,
            ErrorCode.SCHEMA_NOT_FOUND,
            ErrorCode.CATALOG_NOT_FOUND,
            ErrorCode.INVALID_ARGUMENT)
        .map(code -> Arguments.of(new BaseException(code, "rejected")));
  }

  /** Failures a table create raises before or within a transaction that is rolled back. */
  static Stream<Arguments> rejectedCreateFailures() {
    return Stream.of(
            ErrorCode.TABLE_ALREADY_EXISTS,
            ErrorCode.SCHEMA_NOT_FOUND,
            ErrorCode.CATALOG_NOT_FOUND,
            ErrorCode.INVALID_ARGUMENT,
            ErrorCode.FAILED_PRECONDITION)
        .map(code -> Arguments.of(new BaseException(code, "rejected")));
  }

  /**
   * Failures after which the database may hold the new metadata location: a commit that failed in
   * flight is wrapped as INTERNAL, and a session close failing after the commit is thrown as-is.
   */
  static Stream<Arguments> unknownOutcomeFailures() {
    return Stream.of(
        Arguments.of(new BaseException(ErrorCode.INTERNAL, "connection lost during commit")),
        Arguments.of(new IllegalStateException("session close failed after commit")));
  }

  @ParameterizedTest
  @MethodSource("rejectedCommitFailures")
  public void updateTableDeletesTheNewMetadataFileWhenTheCommitIsRejected(
      RuntimeException failure) {
    NormalizedURL written = failUpdateTable(failure);

    verify(metadataService).deleteTableMetadata(written, NormalizedURL.from(TABLE_LOCATION));
  }

  @ParameterizedTest
  @MethodSource("unknownOutcomeFailures")
  public void updateTableKeepsTheNewMetadataFileWhenTheCommitOutcomeIsUnknown(
      RuntimeException failure) {
    failUpdateTable(failure);

    verify(metadataService, never()).deleteTableMetadata(any(), any());
  }

  @ParameterizedTest
  @MethodSource("rejectedCreateFailures")
  public void createTableDeletesTheFirstMetadataFileWhenTheCreateIsRejected(
      RuntimeException failure) {
    NormalizedURL written = failCreateTable(failure);

    verify(metadataService).deleteTableMetadata(written, NormalizedURL.from(TABLE_LOCATION));
  }

  @ParameterizedTest
  @MethodSource("unknownOutcomeFailures")
  public void createTableKeepsTheFirstMetadataFileWhenTheCreateOutcomeIsUnknown(
      RuntimeException failure) {
    failCreateTable(failure);

    verify(metadataService, never()).deleteTableMetadata(any(), any());
  }

  @Test
  public void stagedCreateCommitDeletesTheFirstMetadataFileWhenTheTableAlreadyExists() {
    BaseException exists = new BaseException(ErrorCode.TABLE_ALREADY_EXISTS, "exists");
    when(tableRepository.createTableForIceberg(any(), any())).thenThrow(exists);

    assertThatThrownBy(() -> service.updateTable("cat", "ns", "tbl", stagedCreateCommit()))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Requirement failed: table already exists: ns.tbl");
    NormalizedURL written = writtenMetadataLocation();
    verify(metadataService).deleteTableMetadata(written, NormalizedURL.from(TABLE_LOCATION));
  }

  @ParameterizedTest
  @MethodSource("unknownOutcomeFailures")
  public void stagedCreateCommitKeepsTheFirstMetadataFileWhenTheCreateOutcomeIsUnknown(
      RuntimeException failure) {
    when(tableRepository.createTableForIceberg(any(), any())).thenThrow(failure);

    assertThatThrownBy(() -> service.updateTable("cat", "ns", "tbl", stagedCreateCommit()))
        .isSameAs(failure);
    assertThat(writtenMetadataLocation().toString()).startsWith(TABLE_LOCATION + "/metadata/");
    verify(metadataService, never()).deleteTableMetadata(any(), any());
  }

  @Test
  public void updateTableWithAnUnsupportedColumnTypeWritesNoMetadataFile() {
    TableMetadata base =
        TableMetadata.newTableMetadata(
            SCHEMA, PartitionSpec.unpartitioned(), TABLE_LOCATION, Map.of());
    when(tableRepository.getNativeIcebergTableState("cat", "ns", "tbl"))
        .thenReturn(
            new TableRepository.IcebergTableState(
                UUID.randomUUID(),
                DataSourceFormat.ICEBERG,
                TABLE_LOCATION + "/metadata/00001-" + UUID.randomUUID() + ".metadata.json",
                TABLE_LOCATION));
    when(metadataService.readTableMetadata(any(), any())).thenReturn(base);
    Schema withTimeColumn =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "at", Types.TimeType.get()));
    UpdateTableRequest request =
        new UpdateTableRequest(
            List.of(new UpdateRequirement.AssertTableUUID(base.uuid())),
            List.of(
                new MetadataUpdate.AddSchema(withTimeColumn),
                new MetadataUpdate.SetCurrentSchema(-1)));

    assertThatThrownBy(() -> service.updateTable("cat", "ns", "tbl", request))
        .isInstanceOf(BadRequestException.class)
        .hasMessage("Iceberg type TIME is not supported by Unity Catalog");
    verify(metadataService, never()).writeTableMetadata(any(), any(), any());
    verify(metadataService, never()).deleteTableMetadata(any(), any());
    verify(tableRepository, never())
        .commitIcebergTable(any(), any(), any(), any(), any(), any(), any());
  }

  /** The commit an Iceberg client sends to finish a staged create of an external table. */
  private static UpdateTableRequest stagedCreateCommit() {
    TableMetadata staged =
        TableMetadata.newTableMetadata(
            SCHEMA, PartitionSpec.unpartitioned(), TABLE_LOCATION, Map.of());
    return new UpdateTableRequest(
        List.of(new UpdateRequirement.AssertTableDoesNotExist()), staged.changes());
  }

  /**
   * Commits a property change to a table whose metadata pointer swap fails with {@code failure},
   * checks that the failure reaches the caller unchanged, and returns the metadata file written.
   */
  private NormalizedURL failUpdateTable(RuntimeException failure) {
    String currentLocation =
        TABLE_LOCATION + "/metadata/00001-" + UUID.randomUUID() + ".metadata.json";
    TableMetadata base =
        TableMetadata.newTableMetadata(
            SCHEMA, PartitionSpec.unpartitioned(), TABLE_LOCATION, Map.of());
    when(tableRepository.getNativeIcebergTableState("cat", "ns", "tbl"))
        .thenReturn(
            new TableRepository.IcebergTableState(
                UUID.randomUUID(), DataSourceFormat.ICEBERG, currentLocation, TABLE_LOCATION));
    when(metadataService.readTableMetadata(any(), any())).thenReturn(base);
    doThrow(failure)
        .when(tableRepository)
        .commitIcebergTable(
            eq("cat"), eq("ns"), eq("tbl"), eq(currentLocation), any(), any(), any());
    UpdateTableRequest request =
        new UpdateTableRequest(
            List.of(new UpdateRequirement.AssertTableUUID(base.uuid())),
            List.of(new MetadataUpdate.SetProperties(Map.of("k", "v"))));

    assertThatThrownBy(() -> service.updateTable("cat", "ns", "tbl", request)).isSameAs(failure);
    return writtenMetadataLocation();
  }

  /**
   * Creates an external table whose registration fails with {@code failure}, checks that the
   * failure reaches the caller unchanged, and returns the metadata file written.
   */
  private NormalizedURL failCreateTable(RuntimeException failure) {
    when(tableRepository.createTableForIceberg(any(), any())).thenThrow(failure);
    CreateTableRequest request =
        CreateTableRequest.builder()
            .withName("tbl")
            .withSchema(SCHEMA)
            .withLocation(TABLE_LOCATION)
            .build();

    assertThatThrownBy(() -> service.createTable("cat", "ns", request)).isSameAs(failure);
    return writtenMetadataLocation();
  }

  private NormalizedURL writtenMetadataLocation() {
    ArgumentCaptor<NormalizedURL> written = ArgumentCaptor.forClass(NormalizedURL.class);
    verify(metadataService)
        .writeTableMetadata(any(), written.capture(), eq(NormalizedURL.from(TABLE_LOCATION)));
    return written.getValue();
  }
}
