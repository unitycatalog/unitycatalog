package io.unitycatalog.server.persist;

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
import io.unitycatalog.server.model.ReserveIdentityRangesResponse;
import io.unitycatalog.server.persist.dao.IdentitySequenceDAO;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.ValidationUtils;
import jakarta.persistence.PessimisticLockException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.hibernate.LockMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.exception.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persistence and range-reservation logic for concurrent identity column sequences.
 *
 * <p>Unity Catalog is the authority that guarantees identity uniqueness: it holds the single
 * counter per sequence and hands out disjoint ranges. Writers reserve ranges here and assign values
 * locally, so many writers generate identity values in parallel without contending on a commit.
 *
 * <p>All three operations, create, reserve, and drop, are <b>table-scoped batches</b> of sequences
 * and each executes inside a single transaction, so a batch is <b>all-or-nothing</b>: if any entry
 * is rejected the transaction rolls back and no sequence is modified.
 *
 * <ul>
 *   <li>{@link #createSequences} idempotent create-or-get per entry (and reactivation of a
 *       soft-deleted sequence). Reusing an id requires a matching {@code (start, step)}, and {@code
 *       step} must be non-zero.
 *   <li>{@link #reserveRanges} returns one non-empty, contiguous, inclusive range per entry,
 *       positional with the request, that never overlaps a prior grant and follows the sign of
 *       {@code step}.
 *   <li>{@link #dropSequences} idempotent soft/hard cleanup, one result per unique requested id.
 * </ul>
 */
public class IdentitySequenceRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(IdentitySequenceRepository.class);

  /**
   * Cap on <b>live</b> identity sequences per table. Bounds the fan-out of a single table's
   * identity columns. Soft-deleted sequences do not count for this limit.
   */
  public static final long MAX_SEQUENCES_PER_TABLE = 128;

  /** Maximum length of a client-minted sequence id (opaque string). */
  public static final int MAX_SEQUENCE_ID_LENGTH = 64;

  /**
   * Number of times {@link #reserveRanges} retries a reservation that loses a row-lock race
   * (lock-wait timeout or deadlock) before returning ABORTED.
   */
  public static final int RESERVE_MAX_RETRIES = 4;

  private final Repositories repositories;
  private final SessionFactory sessionFactory;

  public IdentitySequenceRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
  }

  /**
   * Create the requested sequences (or return / reactivate existing ones) atomically. Each entry is
   * idempotent when the stored {@code (start, step)} match the request. A mismatch is a conflict. A
   * matching create against a soft-deleted sequence reactivates it without resetting its counter.
   * If any entry conflicts, or the batch would exceed the per-table cap of live sequences, nothing
   * is created. The returned infos are positional with the request.
   */
  public CreateIdentitySequencesResponse createSequences(CreateIdentitySequences request) {
    ValidationUtils.checkArgument(isNotEmpty(request.getTableId()), "table_id must be set");
    List<IdentitySequenceSpec> specs = request.getSequences();
    ValidationUtils.checkArgument(
        specs != null && !specs.isEmpty(), "sequences must contain at least one entry");

    Set<String> seen = new HashSet<>();
    for (IdentitySequenceSpec spec : specs) {
      validateSequenceId(spec.getSequenceId());
      ValidationUtils.checkArgument(spec.getStart() != null, "start must be set");
      Long step = spec.getStep();
      ValidationUtils.checkArgument(step != null && step != 0L, "step must be a non-zero integer");
      ValidationUtils.checkArgument(
          seen.add(spec.getSequenceId()),
          "Duplicate sequence_id in request: " + spec.getSequenceId());
    }

    return runWithRetry(1, () -> createSequencesOnce(request));
  }

  /**
   * A single create attempt inside one transaction. Retried by once {@link #createSequences} if
   * a concurrent writer wins the insert race for the same {@code (table_id, sequence_id)}. During
   * the retry the now-committed row is resolved by the create-or-get path (an idempotent match, or
   * {@code ALREADY_EXISTS} on a mismatched definition).
   */
  private CreateIdentitySequencesResponse createSequencesOnce(CreateIdentitySequences request) {
    List<IdentitySequenceSpec> specs = request.getSequences();
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          long liveCount = countLiveSequencesForTable(session, request.getTableId());
          List<IdentitySequenceInfo> infos = new ArrayList<>(specs.size());
          List<IdentitySequenceDAO> toPersist = new ArrayList<>();

          for (IdentitySequenceSpec spec : specs) {
            IdentitySequenceDAO existing =
                find(session, request.getTableId(), spec.getSequenceId());
            if (existing != null) {
              boolean matches =
                  existing.getStartValue().equals(spec.getStart())
                      && existing.getStep().equals(spec.getStep());
              if (!matches) {
                throw new BaseException(
                    ErrorCode.ALREADY_EXISTS,
                    "Identity sequence already exists with a different definition: "
                        + spec.getSequenceId());
              }
              if (existing.isDeleted()) {
                // Reactivate: clear the tombstone but keep the counter, so previously issued values
                // are never reused.
                existing.setDeleted(false);
                existing.setUpdatedAt(new Date());
              }
              // Otherwise a matching re-create is a no-op that keeps the current counter.
              infos.add(existing.toIdentitySequenceInfo());
            } else {
              Date now = new Date();
              IdentitySequenceDAO dao =
                  IdentitySequenceDAO.builder()
                      .tableId(request.getTableId())
                      .sequenceId(spec.getSequenceId())
                      .startValue(spec.getStart())
                      .step(spec.getStep())
                      .allocationFrontier(null)
                      .deleted(false)
                      .createdAt(now)
                      .updatedAt(now)
                      .build();
              toPersist.add(dao);
              infos.add(dao.toIdentitySequenceInfo());
            }
          }

          if (liveCount + toPersist.size() > MAX_SEQUENCES_PER_TABLE) {
            throw new BaseException(
                ErrorCode.RESOURCE_EXHAUSTED,
                "Creating "
                    + toPersist.size()
                    + " new identity sequence(s) would exceed the maximum of "
                    + MAX_SEQUENCES_PER_TABLE
                    + " per table "
                    + request.getTableId()
                    + ".");
          }

          try {
            toPersist.forEach(session::persist);
            if (!toPersist.isEmpty()) {
              // Flush the inserts now so a concurrent create of the same (table_id, sequence_id)
              // surfaces here as a constraint violation.
              session.flush();
            }
          } catch (ConstraintViolationException e) {
            throw new ConcurrentCreateException(request.getTableId());
          }
          LOGGER.info(
              "Created {} new identity sequence(s) for table {}",
              toPersist.size(),
              request.getTableId());
          return new CreateIdentitySequencesResponse().sequences(infos);
        },
        "Failed to create identity sequences",
        /* readOnly = */ false);
  }

  /**
   * Atomically reserve one range from each requested sequence and advance each counter. Every
   * targeted row is write-locked for the duration of the transaction (in a deterministic order, so
   * overlapping concurrent batches do not deadlock). A reservation that loses a row-lock race is
   * retried up to {@link #RESERVE_MAX_RETRIES} times before it returns ABORTED. Results are
   * positional with the request.
   */
  public ReserveIdentityRangesResponse reserveRanges(ReserveIdentityRanges request) {
    ValidationUtils.checkArgument(isNotEmpty(request.getTableId()), "table_id must be set");
    List<IdentityReservation> reservations = request.getReservations();
    ValidationUtils.checkArgument(
        reservations != null && !reservations.isEmpty(),
        "reservations must contain at least one entry");

    Set<String> seen = new HashSet<>();
    for (IdentityReservation reservation : reservations) {
      ValidationUtils.checkArgument(
          isNotEmpty(reservation.getSequenceId()), "sequence_id must be set");
      Long count = reservation.getCount();
      ValidationUtils.checkArgument(
          count != null && count > 0L, "count must be a positive integer");
      ValidationUtils.checkArgument(
          seen.add(reservation.getSequenceId()),
          "Duplicate sequence_id in request: " + reservation.getSequenceId());
    }

    // Retries on lock contention. If `RESERVE_MAX_RETRIES` attempts fail, the conflict results
    // in an ABORTED response.
    return runWithRetry(RESERVE_MAX_RETRIES, () -> reserveOnce(request));
  }

  /** A single reservation attempt inside one transaction. Retried by {@link #reserveRanges}. */
  private ReserveIdentityRangesResponse reserveOnce(ReserveIdentityRanges request) {
    List<IdentityReservation> reservations = request.getReservations();
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          // Lock every targeted row first, in a deterministic (sorted) order, so two concurrent
          // batches that touch an overlapping set can never deadlock, and the whole set is
          // serialized before we advance any counter.
          Map<String, IdentitySequenceDAO> locked = new HashMap<>();
          List<String> lockOrder =
              reservations.stream()
                  .map(IdentityReservation::getSequenceId)
                  .sorted()
                  .collect(Collectors.toList());
          for (String sequenceId : lockOrder) {
            IdentitySequenceDAO dao = find(session, request.getTableId(), sequenceId);
            if (dao == null) {
              throw new BaseException(
                  ErrorCode.NOT_FOUND, "Identity sequence not found: " + sequenceId);
            }
            lockSequence(session, dao);
            if (dao.isDeleted()) {
              // A soft-deleted sequence issues no ranges until it is reactivated.
              throw new BaseException(
                  ErrorCode.NOT_FOUND, "Identity sequence not found: " + sequenceId);
            }
            locked.put(sequenceId, dao);
          }

          // Compute every range (validating step and overflow) BEFORE advancing any counter, so a
          // failing entry rolls the whole batch back with nothing advanced. Results stay positional
          // with the request.
          List<IdentityIdRange> ranges = new ArrayList<>(reservations.size());
          for (IdentityReservation reservation : reservations) {
            IdentitySequenceDAO dao = locked.get(reservation.getSequenceId());
            long step = dao.getStep();
            if (reservation.getStep() != null && reservation.getStep() != step) {
              throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT,
                  "Requested step "
                      + reservation.getStep()
                      + " does not match the step of sequence "
                      + reservation.getSequenceId()
                      + " ("
                      + step
                      + ")");
            }
            long count = reservation.getCount();
            long rangeStart;
            long rangeEnd;
            try {
              rangeStart =
                  dao.getAllocationFrontier() == null
                      ? dao.getStartValue()
                      : Math.addExact(dao.getAllocationFrontier(), step);
              rangeEnd = Math.addExact(rangeStart, Math.multiplyExact(step, count - 1L));
            } catch (ArithmeticException e) {
              throw new BaseException(
                  ErrorCode.OUT_OF_RANGE,
                  "Reserving "
                      + count
                      + " values from sequence "
                      + reservation.getSequenceId()
                      + " would overflow the range of a 64-bit integer.");
            }
            ranges.add(
                new IdentityIdRange()
                    .sequenceId(dao.getSequenceId())
                    .rangeStart(rangeStart)
                    .rangeEnd(rangeEnd)
                    .step(step));
          }

          // All ranges are valid, so advance the frontiers now.
          Date now = new Date();
          for (IdentityIdRange range : ranges) {
            IdentitySequenceDAO dao = locked.get(range.getSequenceId());
            dao.setAllocationFrontier(range.getRangeEnd());
            dao.setUpdatedAt(now);
          }
          LOGGER.info("Reserved {} range(s) for table {}", ranges.size(), request.getTableId());
          return new ReserveIdentityRangesResponse().ranges(ranges);
        },
        "Failed to reserve identity ranges",
        /* readOnly = */ false);
  }

  /**
   * Idempotently drop the requested sequences, atomically. {@code SOFT} (the default) retires each
   * sequence but keeps its counter for a later reactivation, while {@code HARD} removes it
   * permanently. Duplicate ids are de-duplicated, and one result is returned per unique requested
   * id.
   */
  public DropIdentitySequencesResponse dropSequences(DropIdentitySequences request) {
    ValidationUtils.checkArgument(isNotEmpty(request.getTableId()), "table_id must be set");
    List<String> sequenceIds = request.getSequenceIds();
    ValidationUtils.checkArgument(
        sequenceIds != null && !sequenceIds.isEmpty(),
        "sequence_ids must contain at least one entry");
    for (String sequenceId : sequenceIds) {
      ValidationUtils.checkArgument(isNotEmpty(sequenceId), "sequence_id must be set");
    }
    // Default to SOFT when the mode is omitted.
    boolean hard = request.getDeletionMode() == DeletionMode.HARD;
    // De-duplicate while preserving first-seen order.
    List<String> uniqueIds = new ArrayList<>(new LinkedHashSet<>(sequenceIds));

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          List<DropIdentitySequenceResult> results = new ArrayList<>(uniqueIds.size());
          for (String sequenceId : uniqueIds) {
            IdentitySequenceDAO dao = find(session, request.getTableId(), sequenceId);
            boolean existed;
            if (dao == null) {
              existed = false;
            } else if (hard) {
              // Hard delete removes the sequence whether it was live or already soft-deleted.
              session.remove(dao);
              existed = true;
            } else if (!dao.isDeleted()) {
              // Soft delete a live sequence: keep its frontier, just tombstone it.
              dao.setDeleted(true);
              dao.setUpdatedAt(new Date());
              existed = true;
            } else {
              // Already soft-deleted: an idempotent no-op.
              existed = false;
            }
            results.add(new DropIdentitySequenceResult().sequenceId(sequenceId).existed(existed));
          }
          long changed = results.stream().filter(r -> Boolean.TRUE.equals(r.getExisted())).count();
          LOGGER.info(
              "Dropped ({}) {} identity sequence(s) for table {}",
              hard ? "hard" : "soft",
              changed,
              request.getTableId());
          return new DropIdentitySequencesResponse().results(results);
        },
        "Failed to drop identity sequences",
        /* readOnly = */ false);
  }

  private static IdentitySequenceDAO find(Session session, String tableId, String sequenceId) {
    return session.get(
        IdentitySequenceDAO.class, new IdentitySequenceDAO.PrimaryKey(tableId, sequenceId));
  }

  private long countLiveSequencesForTable(Session session, String tableId) {
    return session
        .createQuery(
            "SELECT COUNT(s) FROM IdentitySequenceDAO s WHERE s.tableId = :tableId AND s.deleted ="
                + " false",
            Long.class)
        .setParameter("tableId", tableId)
        .getSingleResult();
  }

  /**
   * Take a {@code PESSIMISTIC_WRITE} lock on the sequence row so concurrent reservations serialize.
   * {@code refresh} reloads the row under the lock, so the allocation frontier and lifecycle flag
   * read afterwards reflect any reservation or drop that committed while we waited. A lock-wait
   * timeout or deadlock surfaces as a {@link ReservationConflictException}, which {@link
   * #reserveRanges} retries a bounded number of times before it escapes as ABORTED (409).
   */
  private static void lockSequence(Session session, IdentitySequenceDAO dao) {
    try {
      session.refresh(dao, LockMode.PESSIMISTIC_WRITE);
    } catch (PessimisticLockException e) {
      throw new ReservationConflictException(dao.getSequenceId());
    }
  }

  /**
   * Run an attempt, retrying on a retryable conflict: reserve lock contention, or a create that
   * lost the insert race for an id. Once {@code maxRetries} is reached the last conflict propagates
   * unchanged (ABORTED for reserve, ALREADY_EXISTS for create).
   */
  static <T> T runWithRetry(int maxRetries, Supplier<T> attempt) {
    RetryableConflictException lastConflict = null;
    for (int i = 0; i <= maxRetries; i++) {
      try {
        return attempt.get();
      } catch (RetryableConflictException e) {
        lastConflict = e;
      }
    }
    throw lastConflict;
  }

  abstract static class RetryableConflictException extends BaseException {
    RetryableConflictException(ErrorCode errorCode, String message) {
      super(errorCode, message);
    }
  }

  /** A lock conflict on a sequence row (lock-wait timeout). Exhausting retries yields ABORTED. */
  static class ReservationConflictException extends RetryableConflictException {
    ReservationConflictException(String sequenceId) {
      super(
          ErrorCode.ABORTED,
          "Concurrent reservation in progress on identity sequence "
              + sequenceId
              + "; retry the request.");
    }
  }

  /** A concurrent create won the insert race for the same sequence. */
  static class ConcurrentCreateException extends RetryableConflictException {
    ConcurrentCreateException(String tableId) {
      super(
          ErrorCode.ALREADY_EXISTS,
          "A sequence in the request was created concurrently under table " + tableId + ".");
    }
  }

  private static void validateSequenceId(String sequenceId) {
    ValidationUtils.checkArgument(isNotEmpty(sequenceId), "sequence_id must be set");
    ValidationUtils.checkArgument(
        sequenceId.length() <= MAX_SEQUENCE_ID_LENGTH,
        "sequence_id must be at most " + MAX_SEQUENCE_ID_LENGTH + " characters");
  }

  private static boolean isNotEmpty(String s) {
    return s != null && !s.isEmpty();
  }
}
