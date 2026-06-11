package io.unitycatalog.server.base.delta;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.model.DeltaCreateStagingTableRequest;
import io.unitycatalog.client.delta.model.DeltaCreateTableRequest;
import io.unitycatalog.client.delta.model.DeltaDomainMetadataUpdates;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.client.delta.model.DeltaPrimitiveType;
import io.unitycatalog.client.delta.model.DeltaProtocol;
import io.unitycatalog.client.delta.model.DeltaRowTrackingDomainMetadata;
import io.unitycatalog.client.delta.model.DeltaStagingTableResponse;
import io.unitycatalog.client.delta.model.DeltaStructField;
import io.unitycatalog.client.delta.model.DeltaStructFieldMetadata;
import io.unitycatalog.client.delta.model.DeltaStructType;
import io.unitycatalog.client.delta.model.DeltaTableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.service.delta.DeltaConsts.TableFeature;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.service.delta.UcManagedDeltaContract;
import io.unitycatalog.server.utils.TestUtils;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;

/**
 * Test scaffolding for integration tests that exercise the Delta APIs against a UC catalog.
 */
public abstract class DeltaBaseTableCRUDTestEnv extends BaseTableCRUDTestEnv {

  /**
   * Engine-generated property placeholder timestamp. The UC catalog-managed contract validates
   * presence + non-null only, so any fixed value works for fixtures.
   */
  protected static final long PLACEHOLDER_LAST_COMMIT_TIMESTAMP_MS = 1700000000000L;

  protected DeltaTablesApi deltaTablesApi;

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    deltaTablesApi = new DeltaTablesApi(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected TableOperations createTableOperations(ServerConfig serverConfig) {
    // Delta REST tests reach the table layer through deltaTablesApi; UC's TableOperations is not
    // exercised by the helpers in this class. Override in subclasses that also need it.
    return null;
  }

  /**
   * What the Delta create helpers return; carries the post-create etag so tests don't need to
   * re-load it to satisfy {@code assert-etag} requirements on subsequent updates.
   */
  public record Handle(String name, UUID tableId, String etag) {
    public Handle withEtag(String etag) {
      return new Handle(name, tableId, etag);
    }
  }

  /** Stage a MANAGED Delta table via Delta REST. */
  protected DeltaStagingTableResponse createDeltaStaging(String name) throws ApiException {
    return deltaTablesApi.createStagingTable(
        TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME,
            new DeltaCreateStagingTableRequest().name(name));
  }

  /**
   * Stage and finalize a MANAGED Delta table via Delta REST. Stages internally; use {@link
   * #createDeltaManaged(String, DeltaStagingTableResponse, Map)} if the test needs to retain or
   * inspect the staging response between staging and finalize (e.g. to assert intermediate state
   * against the staging row).
   *
   * <p>Always seeds a {@code deltaRowTracking} domain so update-side tests can exercise
   * remove-domain-metadata against a non-empty state; callers that don't care are unaffected.
   */
  protected Handle createDeltaManaged(String tableName, Map<String, String> extraProperties)
      throws ApiException {
    return createDeltaManaged(tableName, createDeltaStaging(tableName), extraProperties);
  }

  /** Finalize a MANAGED Delta table against an already-created {@code staging} response. */
  protected Handle createDeltaManaged(
      String tableName, DeltaStagingTableResponse staging, Map<String, String> extraProperties)
      throws ApiException {
    Map<String, String> properties =
        new HashMap<>(managedContractProperties(staging.getTableId().toString()));
    properties.putAll(extraProperties);
    DeltaLoadTableResponse resp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            new DeltaCreateTableRequest()
                .name(tableName)
                .location(staging.getLocation())
                .tableType(DeltaTableType.MANAGED)
                .columns(simpleSchema())
                .protocol(managedProtocol())
                .domainMetadata(
                    new DeltaDomainMetadataUpdates()
                        .deltaRowTracking(
                            new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(99L)))
                .lastCommitTimestampMs(PLACEHOLDER_LAST_COMMIT_TIMESTAMP_MS)
                .properties(properties));
    return new Handle(tableName, staging.getTableId(), resp.getMetadata().getEtag());
  }

  /** Create an EXTERNAL Delta table at a fresh local-FS storage path via Delta REST. */
  @SneakyThrows
  protected Handle createDeltaExternal(String tableName) {
    String location = Files.createTempDirectory(testDirectoryRoot, "external_").toString();
    DeltaLoadTableResponse resp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            new DeltaCreateTableRequest()
                .name(tableName)
                .location(location)
                .tableType(DeltaTableType.EXTERNAL)
                .columns(simpleSchema())
                .protocol(
                    new DeltaProtocol()
                        .minReaderVersion(3)
                        .minWriterVersion(7)
                        .readerFeatures(List.of(TableFeature.DELETION_VECTORS.specName()))
                        .writerFeatures(List.of(TableFeature.DELETION_VECTORS.specName())))
                .lastCommitTimestampMs(PLACEHOLDER_LAST_COMMIT_TIMESTAMP_MS)
                .properties(Map.of("delta.enableDeletionVectors", "true")));
    return new Handle(tableName, resp.getMetadata().getTableUuid(), resp.getMetadata().getEtag());
  }

  /** Canonical {@code (id long not null, amount double nullable)} columns. */
  public static DeltaStructType simpleSchema() {
    return new DeltaStructType()
        .type("struct")
        .fields(
            List.of(
                new DeltaStructField()
                    .name("id")
                    .type(new DeltaPrimitiveType().type("long"))
                    .nullable(false)
                    .metadata(new DeltaStructFieldMetadata()),
                new DeltaStructField()
                    .name("amount")
                    .type(new DeltaPrimitiveType().type("double"))
                    .nullable(true)
                    .metadata(new DeltaStructFieldMetadata())));
  }

  /**
   * Full UC catalog-managed protocol satisfying the create-time feature contract, plus {@code
   * rowTracking} so callers may seed a {@code deltaRowTracking} domain at create time (Delta
   * requires the matching writer feature to back the domain metadata).
   */
  public static DeltaProtocol managedProtocol() {
    List<String> writerFeatures = new ArrayList<>(UcManagedDeltaContract.REQUIRED_WRITER_FEATURES);
    writerFeatures.add(TableFeature.ROW_TRACKING.specName());
    return new DeltaProtocol()
        .minReaderVersion(UcManagedDeltaContract.REQUIRED_MIN_READER_VERSION)
        .minWriterVersion(UcManagedDeltaContract.REQUIRED_MIN_WRITER_VERSION)
        .readerFeatures(UcManagedDeltaContract.REQUIRED_READER_FEATURES)
        .writerFeatures(writerFeatures);
  }

  /** Minimum properties UC requires on a catalog-managed Delta table at create time. */
  public static Map<String, String> managedContractProperties(String tableId) {
    Map<String, String> props = new HashMap<>(UcManagedDeltaContract.REQUIRED_FIXED_PROPERTIES);
    props.put(TableProperties.UC_TABLE_ID, tableId);
    // Engine-generated values; the contract only checks non-null, any placeholder works.
    for (String key : UcManagedDeltaContract.ENGINE_GENERATED_PROPERTY_KEYS) {
      props.put(key, "0");
    }
    return props;
  }

  /** Expected {@code delta.feature.*} projection from a protocol's reader + writer features. */
  protected static Map<String, String> featurePropertiesOf(DeltaProtocol protocol) {
    Set<String> names = new HashSet<>(protocol.getReaderFeatures());
    names.addAll(protocol.getWriterFeatures());
    return names.stream()
        .collect(Collectors.toMap(n -> TableProperties.FEATURE_PREFIX + n, n -> "supported"));
  }

  /** The subset of {@code properties} whose keys start with {@code delta.feature.}. */
  protected static Map<String, String> featurePropertiesIn(Map<String, String> properties) {
    return properties.entrySet().stream()
        .filter(e -> e.getKey().startsWith(TableProperties.FEATURE_PREFIX))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
