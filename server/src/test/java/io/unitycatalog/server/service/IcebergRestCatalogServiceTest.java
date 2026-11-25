package io.unitycatalog.server.service;

import static io.unitycatalog.server.service.credential.CredentialContext.READ_ONLY;
import static io.unitycatalog.server.service.credential.CredentialContext.READ_WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.KeyMapper;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.StagingTableRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.service.iceberg.MetadataService;
import io.unitycatalog.server.service.iceberg.TableConfigService;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.types.Types;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

public class IcebergRestCatalogServiceTest {

  private static final String TABLE_LOCATION = "s3://bucket/table";

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  private static final Map<String, String> DELETE_AFTER_COMMIT_KEEPING_ONE =
      Map.of(
          TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true",
          TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1");

  /** Views the one store that every {@link InMemoryFileIO} shares. */
  private static final InMemoryFileIO IN_MEMORY = new InMemoryFileIO();

  private final UnityCatalogAuthorizer authorizer = mock();
  private final Repositories repositories = mock();
  private final KeyMapper keyMapper = mock();
  private final UserRepository userRepository = mock();
  private final ServerProperties serverProperties = mock();
  private final TableRepository tableRepository = mock();
  private final TableConfigService tableConfigService = mock();
  private final MetadataService metadataService = mock();
  private final FileOperations fileOperations = mock();
  private final AtomicReference<String> metadataPointer = new AtomicReference<>();
  private IcebergRestCatalogService service;

  @BeforeEach
  public void setUp() {
    when(repositories.getUserRepository()).thenReturn(userRepository);
    when(repositories.getKeyMapper()).thenReturn(keyMapper);
    when(repositories.getCatalogRepository()).thenReturn(mock(CatalogRepository.class));
    when(repositories.getSchemaRepository()).thenReturn(mock(SchemaRepository.class));
    when(repositories.getStagingTableRepository()).thenReturn(mock(StagingTableRepository.class));
    when(repositories.getTableRepository()).thenReturn(tableRepository);
    when(repositories.getSessionFactory()).thenReturn(mock(SessionFactory.class));
    service =
        new IcebergRestCatalogService(
            authorizer, tableConfigService, metadataService, repositories, serverProperties);
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

  /** Table properties, and which of the table's four metadata files exist after three commits. */
  static Stream<Arguments> metadataCleanupSettings() {
    return Stream.of(
        Arguments.of(DELETE_AFTER_COMMIT_KEEPING_ONE, List.of(false, false, true, true)),
        Arguments.of(
            Map.of(
                TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true",
                TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "2"),
            List.of(false, true, true, true)),
        Arguments.of(
            Map.of(
                TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "false",
                TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1"),
            List.of(true, true, true, true)),
        Arguments.of(
            Map.of(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1"),
            List.of(true, true, true, true)));
  }

  @ParameterizedTest
  @MethodSource("metadataCleanupSettings")
  public void updateTableDeletesMetadataFilesDroppedFromTheLogOnlyWhenEnabled(
      Map<String, String> properties, List<Boolean> expectedExists) {
    String first = metadataFile(0);
    IcebergRestCatalogService inMemory = inMemoryMetadataService(newTable(properties), first);

    String second = commitProperty(inMemory, "1");
    String third = commitProperty(inMemory, "2");
    String current = commitProperty(inMemory, "3");

    assertThat(Stream.of(first, second, third, current).map(IN_MEMORY::fileExists).toList())
        .isEqualTo(expectedExists);
  }

  @Test
  public void updateTableKeepsTheMetadataLogWhenTheCommitIsRejected() {
    String first = metadataFile(0);
    IcebergRestCatalogService inMemory =
        inMemoryMetadataService(newTable(DELETE_AFTER_COMMIT_KEEPING_ONE), first);
    String second = commitProperty(inMemory, "1");
    String current = commitProperty(inMemory, "2");
    BaseException conflict = new BaseException(ErrorCode.UPDATE_REQUIREMENT_CONFLICT, "conflict");
    doThrow(conflict)
        .when(tableRepository)
        .commitIcebergTable(any(), any(), any(), any(), any(), any(), any());

    assertThatThrownBy(() -> commitProperty(inMemory, "3")).isSameAs(conflict);
    assertThat(Stream.of(first, second, current).map(IN_MEMORY::fileExists).toList())
        .containsExactly(false, true, true);
  }

  @Test
  public void updateTableDeletesOnlyDroppedMetadataFilesInsideTheTableLocation() {
    String outside = "s3://bucket/other-table/metadata/" + UUID.randomUUID() + ".metadata.json";
    String inside = metadataFile(0);
    IN_MEMORY.addFile(outside, new byte[] {1});
    IN_MEMORY.addFile(inside, new byte[] {1});
    TableMetadata withOutsideEntry =
        TableMetadata.buildFrom(
                newTable(Map.of(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true")))
            .setPreviousFileLocation(outside)
            .setProperties(Map.of("k", "0"))
            .build();
    TableMetadata withBothEntries =
        TableMetadata.buildFrom(withOutsideEntry)
            .setPreviousFileLocation(inside)
            .setProperties(Map.of("k", "1"))
            .build();
    String base = metadataFile(1);
    IcebergRestCatalogService inMemory = inMemoryMetadataService(withBothEntries, base);

    inMemory.updateTable(
        "cat",
        "ns",
        "tbl",
        new UpdateTableRequest(
            List.of(),
            List.of(
                new MetadataUpdate.SetProperties(
                    Map.of(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1")))));

    assertThat(Stream.of(outside, inside, base).map(IN_MEMORY::fileExists).toList())
        .containsExactly(true, false, true);
  }

  @Test
  public void updateTableSucceedsWhenDeletingADroppedMetadataFileFails() {
    String first = metadataFile(0);
    IcebergRestCatalogService inMemory =
        inMemoryMetadataService(newTable(DELETE_AFTER_COMMIT_KEEPING_ONE), first);
    String second = commitProperty(inMemory, "1");
    when(fileOperations.getFileIO(any(), any()))
        .thenAnswer(
            invocation ->
                new InMemoryFileIO() {
                  @Override
                  public void deleteFile(String path) {
                    throw new IllegalStateException("storage unavailable");
                  }
                });
    String third = commitProperty(inMemory, "2");
    // Each commit asks for a writable FileIO twice: to write the new file, then to delete.
    when(fileOperations.getFileIO(any(), any()))
        .thenAnswer(invocation -> new InMemoryFileIO())
        .thenThrow(new BaseException(ErrorCode.INTERNAL, "credentials unavailable"));
    String current = commitProperty(inMemory, "3");

    assertThat(Stream.of(first, second, third, current).map(IN_MEMORY::fileExists).toList())
        .containsExactly(true, true, true, true);
  }

  private static TableMetadata newTable(Map<String, String> properties) {
    return TableMetadata.newTableMetadata(
        SCHEMA, PartitionSpec.unpartitioned(), TABLE_LOCATION, properties);
  }

  private static String metadataFile(int version) {
    return String.format(
        "%s/metadata/%05d-%s.metadata.json", TABLE_LOCATION, version, UUID.randomUUID());
  }

  /**
   * Returns a service that keeps metadata files in {@link #IN_MEMORY}, for a table whose current
   * metadata {@code metadata} is written at {@code metadataLocation}. The table's metadata pointer
   * is {@link #metadataPointer}, which a successful commit moves.
   */
  private IcebergRestCatalogService inMemoryMetadataService(
      TableMetadata metadata, String metadataLocation) {
    when(fileOperations.getFileIO(any())).thenAnswer(invocation -> new InMemoryFileIO());
    when(fileOperations.getFileIO(any(), any())).thenAnswer(invocation -> new InMemoryFileIO());
    MetadataService inMemoryMetadata = new MetadataService(fileOperations);
    inMemoryMetadata.writeTableMetadata(
        metadata, NormalizedURL.from(metadataLocation), NormalizedURL.from(TABLE_LOCATION));
    metadataPointer.set(metadataLocation);
    when(tableRepository.getNativeIcebergTableState("cat", "ns", "tbl"))
        .thenAnswer(
            invocation ->
                new TableRepository.IcebergTableState(
                    UUID.randomUUID(),
                    DataSourceFormat.ICEBERG,
                    metadataPointer.get(),
                    TABLE_LOCATION));
    doAnswer(invocation -> metadataPointer.getAndSet(invocation.getArgument(4).toString()))
        .when(tableRepository)
        .commitIcebergTable(eq("cat"), eq("ns"), eq("tbl"), any(), any(), any(), any());
    return new IcebergRestCatalogService(
        authorizer, tableConfigService, inMemoryMetadata, repositories, serverProperties);
  }

  /** Commits a new value of one table property and returns the table's metadata location. */
  private String commitProperty(IcebergRestCatalogService icebergService, String value) {
    icebergService.updateTable(
        "cat",
        "ns",
        "tbl",
        new UpdateTableRequest(
            List.of(), List.of(new MetadataUpdate.SetProperties(Map.of("k", value)))));
    return metadataPointer.get();
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
