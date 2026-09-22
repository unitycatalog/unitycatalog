package io.unitycatalog.server.sdk.identitysequence;

import static io.unitycatalog.server.utils.TestUtils.assertDeltaApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.DeltaIdentitySequencesApi;
import io.unitycatalog.client.delta.model.DeltaCreateIdentitySequences;
import io.unitycatalog.client.delta.model.DeltaDropIdentitySequenceResult;
import io.unitycatalog.client.delta.model.DeltaDropIdentitySequences;
import io.unitycatalog.client.delta.model.DeltaErrorType;
import io.unitycatalog.client.delta.model.DeltaIdentityIdRange;
import io.unitycatalog.client.delta.model.DeltaIdentityReservation;
import io.unitycatalog.client.delta.model.DeltaIdentitySequenceSpec;
import io.unitycatalog.client.delta.model.DeltaReserveIdentityRanges;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for the identity sequence endpoints of the UC Delta API, using the generated
 * Delta Java client over HTTP. Runs the full stack: Armeria routing, authorization middleware, the
 * service, and repository.
 */
public class SdkIdentitySequenceCRUDTest extends BaseTableCRUDTestEnv {

  private static final String CATALOG = TestUtils.CATALOG_NAME;
  private static final String SCHEMA = TestUtils.SCHEMA_NAME;
  private static final String TABLE = TestUtils.TABLE_NAME;

  private DeltaIdentitySequencesApi identitySequencesApi;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new SdkSchemaOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected TableOperations createTableOperations(ServerConfig config) {
    return new SdkTableOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    // The identity-sequence service is off by default. We enable it for these tests.
    serverProperties.setProperty(Property.IDENTITY_SEQUENCES_ENABLED.getKey(), "true");
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    identitySequencesApi = new DeltaIdentitySequencesApi(TestUtils.createApiClient(serverConfig));
    // The endpoints address the table by name, so it must exist for the name to resolve.
    createTestingTable(TABLE, TableType.MANAGED, Optional.empty(), tableOperations);
  }

  private void create(String sequenceId, long start, long step) throws ApiException {
    identitySequencesApi.createIdentitySequences(
        CATALOG,
        SCHEMA,
        TABLE,
        new DeltaCreateIdentitySequences()
            .addSequencesItem(
                new DeltaIdentitySequenceSpec().sequenceId(sequenceId).start(start).step(step)));
  }

  private DeltaIdentityIdRange reserve(String sequenceId, long count) throws ApiException {
    return identitySequencesApi
        .reserveIdentityRanges(
            CATALOG,
            SCHEMA,
            TABLE,
            new DeltaReserveIdentityRanges()
                .addReservationsItem(
                    new DeltaIdentityReservation().sequenceId(sequenceId).count(count)))
        .getRanges()
        .get(0);
  }

  private DeltaDropIdentitySequenceResult drop(String sequenceId) throws ApiException {
    return identitySequencesApi
        .dropIdentitySequences(
            CATALOG, SCHEMA, TABLE, new DeltaDropIdentitySequences().addSequenceIdsItem(sequenceId))
        .getResults()
        .get(0);
  }

  @Test
  public void createReserveAndDropRoundTrip() throws ApiException {
    String seq = UUID.randomUUID().toString();

    create(seq, 100L, 2L);

    // First reserve issues start. The second reserve continues with no overlap.
    DeltaIdentityIdRange first = reserve(seq, 3); // 100, 102, 104
    assertThat(first.getSequenceId()).isEqualTo(seq);
    assertThat(first.getRangeStart()).isEqualTo(100L);
    assertThat(first.getRangeEnd()).isEqualTo(104L);
    assertThat(first.getStep()).isEqualTo(2L);

    DeltaIdentityIdRange second = reserve(seq, 2); // 106, 108
    assertThat(second.getRangeStart()).isEqualTo(106L);
    assertThat(second.getRangeEnd()).isEqualTo(108L);

    // Drop is idempotent: first drop removes it. The second drop reports that it was already gone.
    DeltaDropIdentitySequenceResult dropped = drop(seq);
    assertThat(dropped.getSequenceId()).isEqualTo(seq);
    assertThat(dropped.getExisted()).isTrue();

    assertThat(drop(seq).getExisted()).isFalse();

    // Reserving from the now-dropped sequence throws a not-found error.
    assertDeltaApiException(() -> reserve(seq, 1), DeltaErrorType.NOT_FOUND_EXCEPTION, "not found");
  }

  @Test
  public void createIsIdempotentButConflictsOnMismatch() throws ApiException {
    String seq = UUID.randomUUID().toString();
    create(seq, 1L, 1L);
    reserve(seq, 5); // Advance the counter.

    // Re-create with the same definition is a no-op. It must not reset the counter.
    create(seq, 1L, 1L);
    assertThat(reserve(seq, 1).getRangeStart()).isEqualTo(6L);

    // Re-create with a different definition conflicts.
    assertDeltaApiException(
        () -> create(seq, 1L, 2L), DeltaErrorType.ALREADY_EXISTS_EXCEPTION, "already exists");
  }

  @Test
  public void createRejectsZeroStep() {
    assertDeltaApiException(
        () -> create(UUID.randomUUID().toString(), 0L, 0L),
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "step");
  }

  @Test
  public void reserveRejectsNonPositiveCount() throws ApiException {
    String seq = UUID.randomUUID().toString();
    create(seq, 1L, 1L);
    assertDeltaApiException(
        () -> reserve(seq, 0), DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION, "count");
  }

  @Test
  public void reserveOnMissingSequenceIsNotFound() {
    // A valid table (so authorization passes) but an unknown sequence id.
    assertDeltaApiException(
        () -> reserve(UUID.randomUUID().toString(), 1),
        DeltaErrorType.NOT_FOUND_EXCEPTION,
        "not found");
  }

  @Test
  public void reserveOverflowIsRejected() throws ApiException {
    String seq = UUID.randomUUID().toString();
    create(seq, Long.MAX_VALUE - 1, 1L);
    assertDeltaApiException(
        () -> reserve(seq, 3), DeltaErrorType.BAD_REQUEST_EXCEPTION, "overflow");
  }

  @Test
  public void reserveBatchIsPositional() throws ApiException {
    String a = UUID.randomUUID().toString();
    String b = UUID.randomUUID().toString();
    identitySequencesApi.createIdentitySequences(
        CATALOG,
        SCHEMA,
        TABLE,
        new DeltaCreateIdentitySequences()
            .addSequencesItem(new DeltaIdentitySequenceSpec().sequenceId(a).start(100L).step(1L))
            .addSequencesItem(new DeltaIdentitySequenceSpec().sequenceId(b).start(0L).step(10L)));

    List<DeltaIdentityIdRange> ranges =
        identitySequencesApi
            .reserveIdentityRanges(
                CATALOG,
                SCHEMA,
                TABLE,
                new DeltaReserveIdentityRanges()
                    .addReservationsItem(new DeltaIdentityReservation().sequenceId(a).count(2L))
                    .addReservationsItem(new DeltaIdentityReservation().sequenceId(b).count(3L)))
            .getRanges();
    assertThat(ranges).hasSize(2);
    assertThat(ranges.get(0).getSequenceId()).isEqualTo(a);
    assertThat(ranges.get(0).getRangeStart()).isEqualTo(100L);
    assertThat(ranges.get(0).getRangeEnd()).isEqualTo(101L);
    assertThat(ranges.get(1).getSequenceId()).isEqualTo(b);
    assertThat(ranges.get(1).getRangeStart()).isEqualTo(0L);
    assertThat(ranges.get(1).getRangeEnd()).isEqualTo(20L);
  }

  @Test
  public void reserveBatchIsAtomicOnOverflow() throws ApiException {
    String good = UUID.randomUUID().toString();
    String overflowing = UUID.randomUUID().toString();
    create(good, 0L, 1L);
    create(overflowing, Long.MAX_VALUE - 1, 1L);

    // A batch where one reservation overflows must advance neither sequence.
    assertDeltaApiException(
        () ->
            identitySequencesApi.reserveIdentityRanges(
                CATALOG,
                SCHEMA,
                TABLE,
                new DeltaReserveIdentityRanges()
                    .addReservationsItem(new DeltaIdentityReservation().sequenceId(good).count(5L))
                    .addReservationsItem(
                        new DeltaIdentityReservation().sequenceId(overflowing).count(3L))),
        DeltaErrorType.BAD_REQUEST_EXCEPTION,
        "overflow");

    // The good sequence still starts at its start value. The failed batch did not advance it.
    assertThat(reserve(good, 1).getRangeStart()).isEqualTo(0L);
  }

  @Test
  public void dropRemovesPermanentlyAndReCreateStartsFresh() throws ApiException {
    String seq = UUID.randomUUID().toString();
    create(seq, 1L, 1L);
    reserve(seq, 5); // Frontier is at 5.

    assertThat(drop(seq).getExisted()).isTrue();
    assertDeltaApiException(() -> reserve(seq, 1), DeltaErrorType.NOT_FOUND_EXCEPTION, "not found");

    // Re-creating the sequence starts a new counter at the start, not the old frontier.
    create(seq, 1L, 1L);
    assertThat(reserve(seq, 1).getRangeStart()).isEqualTo(1L);
  }
}
