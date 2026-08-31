package io.unitycatalog.server.sdk.identitysequence;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.IdentitySequencesApi;
import io.unitycatalog.client.model.CreateIdentitySequences;
import io.unitycatalog.client.model.DeletionMode;
import io.unitycatalog.client.model.DropIdentitySequenceResult;
import io.unitycatalog.client.model.DropIdentitySequences;
import io.unitycatalog.client.model.IdentityIdRange;
import io.unitycatalog.client.model.IdentityReservation;
import io.unitycatalog.client.model.IdentitySequenceInfo;
import io.unitycatalog.client.model.IdentitySequenceSpec;
import io.unitycatalog.client.model.ReserveIdentityRanges;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.exception.ErrorCode;
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
 * End-to-end tests for the identity sequence service, using the generated Java client over HTTP.
 * Runs the full stack: Armeria routing, authorization middleware, the service, and repository.
 */
public class SdkIdentitySequenceCRUDTest extends BaseTableCRUDTestEnv {

  private IdentitySequencesApi identitySequencesApi;
  private String tableId;

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
    identitySequencesApi = new IdentitySequencesApi(TestUtils.createApiClient(serverConfig));
    tableId =
        createTestingTable(
                TestUtils.TABLE_NAME, TableType.MANAGED, Optional.empty(), tableOperations)
            .getTableId();
  }

  private IdentitySequenceInfo create(String sequenceId, long start, long step)
      throws ApiException {
    return identitySequencesApi
        .createIdentitySequences(
            new CreateIdentitySequences()
                .tableId(tableId)
                .addSequencesItem(
                    new IdentitySequenceSpec().sequenceId(sequenceId).start(start).step(step)))
        .getSequences()
        .get(0);
  }

  private IdentityIdRange reserve(String sequenceId, long count) throws ApiException {
    return identitySequencesApi
        .reserveIdentityRanges(
            new ReserveIdentityRanges()
                .tableId(tableId)
                .addReservationsItem(new IdentityReservation().sequenceId(sequenceId).count(count)))
        .getRanges()
        .get(0);
  }

  @Test
  public void createReserveAndDropRoundTrip() throws ApiException {
    String seq = UUID.randomUUID().toString();

    IdentitySequenceInfo info = create(seq, 100L, 2L);
    assertThat(info.getSequenceId()).isEqualTo(seq);
    assertThat(info.getTableId()).isEqualTo(tableId);
    assertThat(info.getStart()).isEqualTo(100L);
    assertThat(info.getStep()).isEqualTo(2L);

    // First reserve issues start. The second reserve continues with no overlap.
    IdentityIdRange first = reserve(seq, 3); // 100, 102, 104
    assertThat(first.getSequenceId()).isEqualTo(seq);
    assertThat(first.getRangeStart()).isEqualTo(100L);
    assertThat(first.getRangeEnd()).isEqualTo(104L);
    assertThat(first.getStep()).isEqualTo(2L);

    IdentityIdRange second = reserve(seq, 2); // 106, 108
    assertThat(second.getRangeStart()).isEqualTo(106L);
    assertThat(second.getRangeEnd()).isEqualTo(108L);

    // Drop is idempotent: first drop removes it. The second drop reports that it was already gone.
    DropIdentitySequenceResult dropped =
        identitySequencesApi
            .dropIdentitySequences(
                new DropIdentitySequences().tableId(tableId).addSequenceIdsItem(seq))
            .getResults()
            .get(0);
    assertThat(dropped.getSequenceId()).isEqualTo(seq);
    assertThat(dropped.getExisted()).isTrue();

    DropIdentitySequenceResult droppedAgain =
        identitySequencesApi
            .dropIdentitySequences(
                new DropIdentitySequences().tableId(tableId).addSequenceIdsItem(seq))
            .getResults()
            .get(0);
    assertThat(droppedAgain.getExisted()).isFalse();

    // Reserving from the now-dropped sequence throws a not-found error.
    assertApiException(() -> reserve(seq, 1), ErrorCode.NOT_FOUND, "not found");
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
    assertApiException(() -> create(seq, 1L, 2L), ErrorCode.ALREADY_EXISTS, "already exists");
  }

  @Test
  public void createRejectsZeroStep() {
    assertApiException(
        () -> create(UUID.randomUUID().toString(), 0L, 0L), ErrorCode.INVALID_ARGUMENT, "step");
  }

  @Test
  public void reserveRejectsNonPositiveCount() throws ApiException {
    String seq = UUID.randomUUID().toString();
    create(seq, 1L, 1L);
    assertApiException(() -> reserve(seq, 0), ErrorCode.INVALID_ARGUMENT, "count");
  }

  @Test
  public void reserveOnMissingSequenceIsNotFound() {
    // A valid table (so authorization passes) but an unknown sequence id.
    assertApiException(
        () -> reserve(UUID.randomUUID().toString(), 1), ErrorCode.NOT_FOUND, "not found");
  }

  @Test
  public void reserveOverflowIsRejected() throws ApiException {
    String seq = UUID.randomUUID().toString();
    create(seq, Long.MAX_VALUE - 1, 1L);
    assertApiException(() -> reserve(seq, 3), ErrorCode.OUT_OF_RANGE, "overflow");
  }

  @Test
  public void createAndReserveBatchArePositional() throws ApiException {
    String a = UUID.randomUUID().toString();
    String b = UUID.randomUUID().toString();

    List<IdentitySequenceInfo> created =
        identitySequencesApi
            .createIdentitySequences(
                new CreateIdentitySequences()
                    .tableId(tableId)
                    .addSequencesItem(new IdentitySequenceSpec().sequenceId(a).start(100L).step(1L))
                    .addSequencesItem(new IdentitySequenceSpec().sequenceId(b).start(0L).step(10L)))
            .getSequences();
    assertThat(created).hasSize(2);
    assertThat(created.get(0).getSequenceId()).isEqualTo(a);
    assertThat(created.get(1).getSequenceId()).isEqualTo(b);

    List<IdentityIdRange> ranges =
        identitySequencesApi
            .reserveIdentityRanges(
                new ReserveIdentityRanges()
                    .tableId(tableId)
                    .addReservationsItem(new IdentityReservation().sequenceId(a).count(2L))
                    .addReservationsItem(new IdentityReservation().sequenceId(b).count(3L)))
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
    assertApiException(
        () ->
            identitySequencesApi.reserveIdentityRanges(
                new ReserveIdentityRanges()
                    .tableId(tableId)
                    .addReservationsItem(new IdentityReservation().sequenceId(good).count(5L))
                    .addReservationsItem(
                        new IdentityReservation().sequenceId(overflowing).count(3L))),
        ErrorCode.OUT_OF_RANGE,
        "overflow");

    // The good sequence still starts at its start value. The failed batch did not advance it.
    assertThat(reserve(good, 1).getRangeStart()).isEqualTo(0L);
  }

  @Test
  public void softDeleteBlocksReserveThenReactivates() throws ApiException {
    String seq = UUID.randomUUID().toString();
    create(seq, 1L, 1L);
    reserve(seq, 5); // Frontier is at 5.

    // Default drop is SOFT which blocks reserving while the sequence is "soft-deleted".
    identitySequencesApi.dropIdentitySequences(
        new DropIdentitySequences().tableId(tableId).addSequenceIdsItem(seq));
    assertApiException(() -> reserve(seq, 1), ErrorCode.NOT_FOUND, "not found");

    // A matching create reactivates the sequence without resetting the counter.
    create(seq, 1L, 1L);
    assertThat(reserve(seq, 1).getRangeStart()).isEqualTo(6L);
  }

  @Test
  public void hardDeleteRemovesPermanently() throws ApiException {
    String seq = UUID.randomUUID().toString();
    create(seq, 1L, 1L);
    reserve(seq, 5);

    DropIdentitySequenceResult result =
        identitySequencesApi
            .dropIdentitySequences(
                new DropIdentitySequences()
                    .tableId(tableId)
                    .addSequenceIdsItem(seq)
                    .deletionMode(DeletionMode.HARD))
            .getResults()
            .get(0);
    assertThat(result.getExisted()).isTrue();

    // Re-creating the sequence starts a new counter at the start, not the old frontier.
    create(seq, 1L, 1L);
    assertThat(reserve(seq, 1).getRangeStart()).isEqualTo(1L);
  }
}
