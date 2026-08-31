package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateIdentitySequences;
import io.unitycatalog.server.model.CreateIdentitySequencesResponse;
import io.unitycatalog.server.model.DeletionMode;
import io.unitycatalog.server.model.DropIdentitySequenceResult;
import io.unitycatalog.server.model.DropIdentitySequences;
import io.unitycatalog.server.model.DropIdentitySequencesResponse;
import io.unitycatalog.server.model.IdentityIdRange;
import io.unitycatalog.server.model.IdentityReservation;
import io.unitycatalog.server.model.IdentitySequenceInfo;
import io.unitycatalog.server.model.IdentitySequenceSpec;
import io.unitycatalog.server.model.ReserveIdentityRanges;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link IdentitySequenceRepository}: the batched create-or-get, range reservation,
 * and drop of the concurrent identity column sequence service. Uses a real (H2) SessionFactory so
 * the pessimistic row lock and the actual SQL run, rather than a mocked session.
 *
 * <p>The suite pins two things in particular: the per-sequence value arithmetic, and that every
 * batch is <b>atomic</b>: a single failing entry must leave every other sequence in the request
 * untouched.
 */
public class IdentitySequenceRepositoryTest {

  private static SessionFactory sessionFactory;
  private static IdentitySequenceRepository repository;

  @BeforeAll
  public static void setUp() {
    ServerProperties serverProperties = new ServerProperties(new Properties());
    HibernateConfigurator hibernateConfigurator = new HibernateConfigurator(serverProperties);
    sessionFactory = hibernateConfigurator.getSessionFactory();
    Repositories repositories = new Repositories(sessionFactory, serverProperties);
    repository = repositories.getIdentitySequenceRepository();
  }

  @AfterAll
  public static void tearDown() {
    sessionFactory.close();
  }

  private static String uniqueId() {
    return UUID.randomUUID().toString();
  }

  private static IdentitySequenceSpec spec(String sequenceId, long start, long step) {
    return new IdentitySequenceSpec().sequenceId(sequenceId).start(start).step(step);
  }

  private void create(String tableId, String sequenceId, long start, long step) {
    repository.createSequences(
        new CreateIdentitySequences()
            .tableId(tableId)
            .addSequencesItem(spec(sequenceId, start, step)));
  }

  private IdentityIdRange reserve(String tableId, String sequenceId, long count) {
    return repository
        .reserveRanges(
            new ReserveIdentityRanges()
                .tableId(tableId)
                .addReservationsItem(new IdentityReservation().sequenceId(sequenceId).count(count)))
        .getRanges()
        .get(0);
  }

  private DropIdentitySequencesResponse drop(String tableId, String... sequenceIds) {
    // No deletion_mode -> defaults to SOFT.
    return dropWithMode(tableId, null, sequenceIds);
  }

  private DropIdentitySequencesResponse dropWithMode(
      String tableId, DeletionMode mode, String... sequenceIds) {
    DropIdentitySequences request = new DropIdentitySequences().tableId(tableId).deletionMode(mode);
    for (String id : sequenceIds) {
      request.addSequenceIdsItem(id);
    }
    return repository.dropSequences(request);
  }

  @Test
  public void firstReserveIssuesStartThenContiguous() {
    String tableId = uniqueId();
    String seq = uniqueId();
    create(tableId, seq, /* start = */ 100L, /* step = */ 1L);

    IdentityIdRange first = reserve(tableId, seq, 5);
    assertThat(first.getSequenceId()).isEqualTo(seq);
    assertThat(first.getRangeStart()).isEqualTo(100L);
    assertThat(first.getRangeEnd()).isEqualTo(104L);
    assertThat(first.getStep()).isEqualTo(1L);

    // Next reservation continues immediately after the previous range, with no overlap or reuse.
    IdentityIdRange second = reserve(tableId, seq, 3);
    assertThat(second.getRangeStart()).isEqualTo(105L);
    assertThat(second.getRangeEnd()).isEqualTo(107L);
  }

  @Test
  public void nonUnitStepIsHonored() {
    String tableId = uniqueId();
    String seq = uniqueId();
    create(tableId, seq, /* start = */ 0L, /* step = */ 10L);

    IdentityIdRange first = reserve(tableId, seq, 3); // 0, 10, 20
    assertThat(first.getRangeStart()).isEqualTo(0L);
    assertThat(first.getRangeEnd()).isEqualTo(20L);

    IdentityIdRange second = reserve(tableId, seq, 2); // 30, 40
    assertThat(second.getRangeStart()).isEqualTo(30L);
    assertThat(second.getRangeEnd()).isEqualTo(40L);
  }

  @Test
  public void negativeStepDescends() {
    String tableId = uniqueId();
    String seq = uniqueId();
    create(tableId, seq, /* start = */ 100L, /* step = */ -5L);

    IdentityIdRange range = reserve(tableId, seq, 3); // 100, 95, 90
    assertThat(range.getRangeStart()).isEqualTo(100L);
    assertThat(range.getRangeEnd()).isEqualTo(90L);
    assertThat(range.getStep()).isEqualTo(-5L);
    // Callers must not assume range_start <= range_end for a descending sequence.
    assertThat(range.getRangeStart()).isGreaterThan(range.getRangeEnd());
  }

  @Test
  public void createIsIdempotentOnMatchingDefinition() {
    String tableId = uniqueId();
    String seq = uniqueId();
    create(tableId, seq, 1L, 1L);
    // Reserve so the sequence has advanced, then re-create with the same definition.
    reserve(tableId, seq, 4);

    CreateIdentitySequencesResponse got =
        repository.createSequences(
            new CreateIdentitySequences().tableId(tableId).addSequencesItem(spec(seq, 1L, 1L)));
    assertThat(got.getSequences()).hasSize(1);
    IdentitySequenceInfo sequenceInfo = got.getSequences().get(0);
    assertThat(sequenceInfo.getSequenceId()).isEqualTo(seq);
    assertThat(sequenceInfo.getStart()).isEqualTo(1L);
    assertThat(sequenceInfo.getStep()).isEqualTo(1L);

    // The re-create must not reset the frontier.
    assertThat(reserve(tableId, seq, 1).getRangeStart()).isEqualTo(5L);
  }

  @Test
  public void createConflictsOnDifferentDefinition() {
    String tableId = uniqueId();
    String seq = uniqueId();
    create(tableId, seq, 1L, 1L);

    assertThatThrownBy(() -> create(tableId, seq, 1L, 2L))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.ALREADY_EXISTS));
  }

  @Test
  public void createRejectsZeroStep() {
    String tableId = uniqueId();
    assertThatThrownBy(() -> create(tableId, uniqueId(), 0L, 0L))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.INVALID_ARGUMENT));
  }

  @Test
  public void createRejectsTooLongSequenceId() {
    String tableId = uniqueId();
    String tooLong = "s".repeat(IdentitySequenceRepository.MAX_SEQUENCE_ID_LENGTH + 1);
    assertThatThrownBy(() -> create(tableId, tooLong, 1L, 1L))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.INVALID_ARGUMENT));
    // The boundary length is accepted.
    create(tableId, "s".repeat(IdentitySequenceRepository.MAX_SEQUENCE_ID_LENGTH), 1L, 1L);
  }

  @Test
  public void reserveRejectsNonPositiveCount() {
    String tableId = uniqueId();
    String seq = uniqueId();
    create(tableId, seq, 1L, 1L);
    assertThatThrownBy(() -> reserve(tableId, seq, 0))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.INVALID_ARGUMENT));
  }

  @Test
  public void reserveRejectsMismatchedStep() {
    String tableId = uniqueId();
    String seq = uniqueId();
    create(tableId, seq, 0L, 2L);
    assertThatThrownBy(
            () ->
                repository.reserveRanges(
                    new ReserveIdentityRanges()
                        .tableId(tableId)
                        .addReservationsItem(
                            new IdentityReservation().sequenceId(seq).count(1L).step(3L))))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.INVALID_ARGUMENT));
  }

  @Test
  public void reserveOnMissingSequenceIsNotFound() {
    assertThatThrownBy(() -> reserve(uniqueId(), uniqueId(), 1))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.NOT_FOUND));
  }

  @Test
  public void reserveFromWrongTableIsNotFound() {
    String tableId = uniqueId();
    String seq = uniqueId();
    create(tableId, seq, 1L, 1L);
    // A sequence id belonging to a different table must not be reservable.
    assertThatThrownBy(() -> reserve(uniqueId(), seq, 1))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.NOT_FOUND));
  }

  @Test
  public void reserveOverflowIsRejected() {
    String tableId = uniqueId();
    String seq = uniqueId();
    create(tableId, seq, Long.MAX_VALUE - 1, 1L);
    // start=MAX-1, so reserving 2 would need MAX-1 and MAX (ok), but reserving 3 overflows.
    assertThatThrownBy(() -> reserve(tableId, seq, 3))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.OUT_OF_RANGE));
  }

  @Test
  public void dropIsIdempotentAndTableScoped() {
    String tableId = uniqueId();
    String seq = uniqueId();
    create(tableId, seq, 1L, 1L);

    // Dropping from a different table is a no-op.
    assertThat(drop(uniqueId(), seq).getResults().get(0).getExisted()).isFalse();

    assertThat(drop(tableId, seq).getResults().get(0).getExisted()).isTrue();
    // Second drop reports the sequence was already gone.
    assertThat(drop(tableId, seq).getResults().get(0).getExisted()).isFalse();

    // And the sequence is truly gone.
    assertThatThrownBy(() -> reserve(tableId, seq, 1))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.NOT_FOUND));
  }

  @Test
  public void perTableSequenceCapacityIsEnforced() {
    String tableId = uniqueId();
    for (int i = 0; i < IdentitySequenceRepository.MAX_SEQUENCES_PER_TABLE; i++) {
      create(tableId, uniqueId(), 1L, 1L);
    }
    assertThatThrownBy(() -> create(tableId, uniqueId(), 1L, 1L))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.RESOURCE_EXHAUSTED));
  }

  @Test
  public void createBatchCreatesAllAndReturnsPositional() {
    String tableId = uniqueId();
    String a = uniqueId();
    String b = uniqueId();
    CreateIdentitySequencesResponse response =
        repository.createSequences(
            new CreateIdentitySequences()
                .tableId(tableId)
                .addSequencesItem(spec(a, 10L, 1L))
                .addSequencesItem(spec(b, 0L, 5L)));

    assertThat(response.getSequences()).hasSize(2);
    assertThat(response.getSequences().get(0).getSequenceId()).isEqualTo(a);
    assertThat(response.getSequences().get(1).getSequenceId()).isEqualTo(b);
    // Both are independently reservable.
    assertThat(reserve(tableId, a, 1).getRangeStart()).isEqualTo(10L);
    assertThat(reserve(tableId, b, 1).getRangeStart()).isEqualTo(0L);
  }

  @Test
  public void createBatchRejectsDuplicateIdInRequest() {
    String tableId = uniqueId();
    String dup = uniqueId();
    assertThatThrownBy(
            () ->
                repository.createSequences(
                    new CreateIdentitySequences()
                        .tableId(tableId)
                        .addSequencesItem(spec(dup, 1L, 1L))
                        .addSequencesItem(spec(dup, 1L, 1L))))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.INVALID_ARGUMENT));
    // Nothing was created.
    assertThatThrownBy(() -> reserve(tableId, dup, 1))
        .satisfies(e -> assertErrorCode(e, ErrorCode.NOT_FOUND));
  }

  @Test
  public void createBatchIsAtomicOnConflict() {
    String tableId = uniqueId();
    String existing = uniqueId();
    String fresh = uniqueId();
    create(tableId, existing, 1L, 1L);

    // A batch that mixes a conflicting re-create with a brand-new sequence must create neither.
    assertThatThrownBy(
            () ->
                repository.createSequences(
                    new CreateIdentitySequences()
                        .tableId(tableId)
                        .addSequencesItem(spec(existing, 1L, 2L)) // conflict: different step
                        .addSequencesItem(spec(fresh, 1L, 1L))))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.ALREADY_EXISTS));

    // The fresh sequence was rolled back with the rest of the batch.
    assertThatThrownBy(() -> reserve(tableId, fresh, 1))
        .satisfies(e -> assertErrorCode(e, ErrorCode.NOT_FOUND));
  }

  @Test
  public void createBatchRejectsEmpty() {
    assertThatThrownBy(
            () -> repository.createSequences(new CreateIdentitySequences().tableId(uniqueId())))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.INVALID_ARGUMENT));
  }

  @Test
  public void createBatchCapacityIsEnforcedAtomically() {
    String tableId = uniqueId();
    // Fill to one below the capacity.
    for (int i = 0; i < IdentitySequenceRepository.MAX_SEQUENCES_PER_TABLE - 1; i++) {
      create(tableId, uniqueId(), 1L, 1L);
    }
    String a = uniqueId();
    String b = uniqueId();
    // A 2-entry batch would push one over the capacity, so neither may be created.
    assertThatThrownBy(
            () ->
                repository.createSequences(
                    new CreateIdentitySequences()
                        .tableId(tableId)
                        .addSequencesItem(spec(a, 1L, 1L))
                        .addSequencesItem(spec(b, 1L, 1L))))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.RESOURCE_EXHAUSTED));

    assertThatThrownBy(() -> reserve(tableId, a, 1))
        .satisfies(e -> assertErrorCode(e, ErrorCode.NOT_FOUND));
    assertThatThrownBy(() -> reserve(tableId, b, 1))
        .satisfies(e -> assertErrorCode(e, ErrorCode.NOT_FOUND));
    // Exactly one more still fits.
    create(tableId, uniqueId(), 1L, 1L);
  }

  @Test
  public void reserveBatchReservesIndependentSequencesPositional() {
    String tableId = uniqueId();
    String a = uniqueId();
    String b = uniqueId();
    create(tableId, a, 100L, 1L);
    create(tableId, b, 0L, 10L);

    // Order the request so its order differs from the sorted lock order, proving results are
    // positional with the request rather than with the (sorted) order rows are locked in.
    String first = a.compareTo(b) > 0 ? a : b;
    String second = a.compareTo(b) > 0 ? b : a;
    ReserveIdentityRangesResponseHolder holder = reserveBatch(tableId, first, 2L, second, 3L);

    assertThat(holder.ranges).hasSize(2);
    assertThat(holder.ranges.get(0).getSequenceId()).isEqualTo(first);
    assertThat(holder.ranges.get(1).getSequenceId()).isEqualTo(second);
  }

  @Test
  public void reserveBatchRejectsDuplicateIdInRequest() {
    String tableId = uniqueId();
    String seq = uniqueId();
    create(tableId, seq, 1L, 1L);
    assertThatThrownBy(
            () ->
                repository.reserveRanges(
                    new ReserveIdentityRanges()
                        .tableId(tableId)
                        .addReservationsItem(new IdentityReservation().sequenceId(seq).count(1L))
                        .addReservationsItem(new IdentityReservation().sequenceId(seq).count(1L))))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.INVALID_ARGUMENT));
    // The sequence was not advanced by the rejected batch.
    assertThat(reserve(tableId, seq, 1).getRangeStart()).isEqualTo(1L);
  }

  @Test
  public void reserveBatchIsAtomicOnOverflow() {
    String tableId = uniqueId();
    String good = uniqueId();
    String overflowing = uniqueId();
    create(tableId, good, 0L, 1L);
    create(tableId, overflowing, Long.MAX_VALUE - 1, 1L);

    assertThatThrownBy(
            () ->
                repository.reserveRanges(
                    new ReserveIdentityRanges()
                        .tableId(tableId)
                        .addReservationsItem(new IdentityReservation().sequenceId(good).count(5L))
                        .addReservationsItem(
                            new IdentityReservation().sequenceId(overflowing).count(3L))))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.OUT_OF_RANGE));

    // Neither sequence was advanced.
    assertThat(reserve(tableId, good, 1).getRangeStart()).isEqualTo(0L);
    assertThat(reserve(tableId, overflowing, 1).getRangeStart()).isEqualTo(Long.MAX_VALUE - 1);
  }

  @Test
  public void reserveBatchIsAtomicOnUnknownSequence() {
    String tableId = uniqueId();
    String good = uniqueId();
    create(tableId, good, 0L, 1L);

    assertThatThrownBy(
            () ->
                repository.reserveRanges(
                    new ReserveIdentityRanges()
                        .tableId(tableId)
                        .addReservationsItem(new IdentityReservation().sequenceId(good).count(5L))
                        .addReservationsItem(
                            new IdentityReservation().sequenceId(uniqueId()).count(1L))))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.NOT_FOUND));

    // The good sequence was NOT advanced.
    assertThat(reserve(tableId, good, 1).getRangeStart()).isEqualTo(0L);
  }

  @Test
  public void reserveBatchRejectsEmpty() {
    assertThatThrownBy(
            () -> repository.reserveRanges(new ReserveIdentityRanges().tableId(uniqueId())))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.INVALID_ARGUMENT));
  }

  @Test
  public void dropBatchDropsMultipleAndDeduplicates() {
    String tableId = uniqueId();
    String a = uniqueId();
    String b = uniqueId();
    create(tableId, a, 1L, 1L);
    create(tableId, b, 1L, 1L);

    // Duplicate id in the request is de-duplicated to a single result.
    DropIdentitySequencesResponse response = drop(tableId, a, b, a);
    assertThat(response.getResults()).hasSize(2);
    assertThat(response.getResults().stream().allMatch(r -> Boolean.TRUE.equals(r.getExisted())))
        .isTrue();

    assertThatThrownBy(() -> reserve(tableId, a, 1))
        .satisfies(e -> assertErrorCode(e, ErrorCode.NOT_FOUND));
    assertThatThrownBy(() -> reserve(tableId, b, 1))
        .satisfies(e -> assertErrorCode(e, ErrorCode.NOT_FOUND));
  }

  @Test
  public void dropBatchReportsPerIdExistedInOrder() {
    String tableId = uniqueId();
    String present = uniqueId();
    String absent = uniqueId();
    create(tableId, present, 1L, 1L);

    List<DropIdentitySequenceResult> results = drop(tableId, present, absent).getResults();
    assertThat(results).hasSize(2);

    DropIdentitySequenceResult presentResult = results.get(0);
    assertThat(presentResult.getSequenceId()).isEqualTo(present);
    assertThat(presentResult.getExisted()).isTrue();

    DropIdentitySequenceResult absentResult = results.get(1);
    assertThat(absentResult.getSequenceId()).isEqualTo(absent);
    assertThat(absentResult.getExisted()).isFalse();
  }

  @Test
  public void dropBatchRejectsEmpty() {
    assertThatThrownBy(
            () -> repository.dropSequences(new DropIdentitySequences().tableId(uniqueId())))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.INVALID_ARGUMENT));
  }

  @Test
  public void sameSequenceIdUnderDifferentTablesAreIndependent() {
    String tableA = uniqueId();
    String tableB = uniqueId();
    String seq = uniqueId(); // the SAME sequence id under two different tables

    create(tableA, seq, 100L, 1L);
    // Creating the same id under a different table is a distinct sequence, not a conflict.
    create(tableB, seq, 500L, 1L);

    // Each advances from its own start, independently.
    assertThat(reserve(tableA, seq, 1).getRangeStart()).isEqualTo(100L);
    assertThat(reserve(tableB, seq, 1).getRangeStart()).isEqualTo(500L);
    assertThat(reserve(tableA, seq, 1).getRangeStart()).isEqualTo(101L);
    assertThat(reserve(tableB, seq, 1).getRangeStart()).isEqualTo(501L);

    // Dropping one table's sequence leaves the other's intact.
    drop(tableA, seq);
    assertThatThrownBy(() -> reserve(tableA, seq, 1))
        .satisfies(e -> assertErrorCode(e, ErrorCode.NOT_FOUND));
    assertThat(reserve(tableB, seq, 1).getRangeStart()).isEqualTo(502L);
  }

  @Test
  public void softDeleteBlocksReserveAndReactivationResumesPastFrontier() {
    String tableId = uniqueId();
    String seq = uniqueId();
    create(tableId, seq, 1L, 1L);
    reserve(tableId, seq, 5); // frontier now at 5

    // Default drop is SOFT, so the sequence rejects reservations while soft-deleted.
    assertThat(drop(tableId, seq).getResults().get(0).getExisted()).isTrue();
    assertThatThrownBy(() -> reserve(tableId, seq, 1))
        .satisfies(e -> assertErrorCode(e, ErrorCode.NOT_FOUND));

    // A matching create reactivates it WITHOUT resetting the counter, so the next value is 6.
    create(tableId, seq, 1L, 1L);
    assertThat(reserve(tableId, seq, 1).getRangeStart()).isEqualTo(6L);
  }

  @Test
  public void reactivationWithDifferentDefinitionConflicts() {
    String tableId = uniqueId();
    String seq = uniqueId();
    create(tableId, seq, 1L, 1L);
    drop(tableId, seq); // Soft delete the sequence.

    assertThatThrownBy(() -> create(tableId, seq, 1L, 2L))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.ALREADY_EXISTS));
  }

  @Test
  public void softDeleteIsIdempotent() {
    String tableId = uniqueId();
    String seq = uniqueId();
    create(tableId, seq, 1L, 1L);

    // First SOFT drop changes state and a second SOFT drop is a no-op.
    assertThat(drop(tableId, seq).getResults().get(0).getExisted()).isTrue();
    assertThat(drop(tableId, seq).getResults().get(0).getExisted()).isFalse();
  }

  @Test
  public void hardDeleteRemovesPermanentlyAndFreesReactivation() {
    String tableId = uniqueId();
    String seq = uniqueId();
    create(tableId, seq, 1L, 1L);
    reserve(tableId, seq, 5); // Frontier at 5.

    assertThat(dropWithMode(tableId, DeletionMode.HARD, seq).getResults().get(0).getExisted())
        .isTrue();
    // Hard delete on an already-absent sequence is a no-op.
    assertThat(dropWithMode(tableId, DeletionMode.HARD, seq).getResults().get(0).getExisted())
        .isFalse();
    assertThatThrownBy(() -> reserve(tableId, seq, 1))
        .satisfies(e -> assertErrorCode(e, ErrorCode.NOT_FOUND));

    // The state is gone: re-creating the id starts a brand-new counter at start (not the old 5).
    // Note that this should never happen in practice because two independent sequences sharing the
    // same id are not allowed.
    create(tableId, seq, 1L, 1L);
    assertThat(reserve(tableId, seq, 1).getRangeStart()).isEqualTo(1L);
  }

  @Test
  public void hardDeleteRemovesASoftDeletedSequence() {
    String tableId = uniqueId();
    String seq = uniqueId();
    create(tableId, seq, 1L, 1L);
    drop(tableId, seq); // soft delete

    // HARD delete removes an already soft-deleted sequence, and reports it changed state.
    assertThat(dropWithMode(tableId, DeletionMode.HARD, seq).getResults().get(0).getExisted())
        .isTrue();
  }

  @Test
  public void softDeleteDoesNotCountAgainstTheLiveCapacity() {
    String tableId = uniqueId();
    String victim = uniqueId();
    create(tableId, victim, 1L, 1L);
    for (int i = 0; i < IdentitySequenceRepository.MAX_SEQUENCES_PER_TABLE - 1; i++) {
      create(tableId, uniqueId(), 1L, 1L);
    }
    // At the live capacity, another create fails.
    String extra = uniqueId();
    assertThatThrownBy(() -> create(tableId, extra, 1L, 1L))
        .satisfies(e -> assertErrorCode(e, ErrorCode.RESOURCE_EXHAUSTED));

    // Soft-deleting one frees a live slot, so the new create then succeeds.
    drop(tableId, victim);
    create(tableId, extra, 1L, 1L);
  }

  @Test
  public void reserveRetriesLockConflictThenSucceeds() {
    // Conflict on every attempt but the last (within budget).
    AtomicInteger attempts = new AtomicInteger();
    String result =
        IdentitySequenceRepository.runWithRetry(
            IdentitySequenceRepository.RESERVE_MAX_RETRIES,
            () -> {
              if (attempts.getAndIncrement() < IdentitySequenceRepository.RESERVE_MAX_RETRIES) {
                throw new IdentitySequenceRepository.ReservationConflictException("seq");
              }
              return "ok";
            });
    assertThat(result).isEqualTo("ok");
    // One initial attempt plus RESERVE_MAX_RETRIES retries.
    assertThat(attempts.get()).isEqualTo(IdentitySequenceRepository.RESERVE_MAX_RETRIES + 1);
  }

  @Test
  public void reserveReturnsAbortedAfterExhaustingRetries() {
    // Conflict on every attempt -> the loop gives up and the conflict escapes as ABORTED.
    AtomicInteger attempts = new AtomicInteger();
    assertThatThrownBy(
            () ->
                IdentitySequenceRepository.runWithRetry(
                    IdentitySequenceRepository.RESERVE_MAX_RETRIES,
                    () -> {
                      attempts.incrementAndGet();
                      throw new IdentitySequenceRepository.ReservationConflictException("seq");
                    }))
        .isInstanceOf(BaseException.class)
        .satisfies(e -> assertErrorCode(e, ErrorCode.ABORTED));
    assertThat(attempts.get()).isEqualTo(IdentitySequenceRepository.RESERVE_MAX_RETRIES + 1);
  }

  /** Small holder so the positional-order assertion reads clearly. */
  private static final class ReserveIdentityRangesResponseHolder {
    final List<IdentityIdRange> ranges;

    ReserveIdentityRangesResponseHolder(List<IdentityIdRange> ranges) {
      this.ranges = ranges;
    }
  }

  private ReserveIdentityRangesResponseHolder reserveBatch(
      String tableId, String seq1, long count1, String seq2, long count2) {
    return new ReserveIdentityRangesResponseHolder(
        repository
            .reserveRanges(
                new ReserveIdentityRanges()
                    .tableId(tableId)
                    .addReservationsItem(new IdentityReservation().sequenceId(seq1).count(count1))
                    .addReservationsItem(new IdentityReservation().sequenceId(seq2).count(count2)))
            .getRanges());
  }

  private static void assertErrorCode(Throwable t, ErrorCode expected) {
    assertThat(((BaseException) t).getErrorCode()).isEqualTo(expected);
  }
}
