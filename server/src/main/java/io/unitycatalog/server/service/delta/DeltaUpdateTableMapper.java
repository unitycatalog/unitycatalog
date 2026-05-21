package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.delta.model.AddCommitUpdate;
import io.unitycatalog.server.delta.model.AssertEtag;
import io.unitycatalog.server.delta.model.AssertTableUUID;
import io.unitycatalog.server.delta.model.DeltaCommit;
import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.DomainMetadataUpdates;
import io.unitycatalog.server.delta.model.RemoveDomainMetadataUpdate;
import io.unitycatalog.server.delta.model.RemovePropertiesUpdate;
import io.unitycatalog.server.delta.model.SetDomainMetadataUpdate;
import io.unitycatalog.server.delta.model.SetLatestBackfilledVersionUpdate;
import io.unitycatalog.server.delta.model.SetPartitionColumnsUpdate;
import io.unitycatalog.server.delta.model.SetPropertiesUpdate;
import io.unitycatalog.server.delta.model.SetProtocolUpdate;
import io.unitycatalog.server.delta.model.SetSchemaUpdate;
import io.unitycatalog.server.delta.model.SetTableCommentUpdate;
import io.unitycatalog.server.delta.model.StructField;
import io.unitycatalog.server.delta.model.StructType;
import io.unitycatalog.server.delta.model.TableRequirement;
import io.unitycatalog.server.delta.model.TableUpdate;
import io.unitycatalog.server.delta.model.UpdateSnapshotVersionUpdate;
import io.unitycatalog.server.delta.model.UpdateTableRequest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.MutablePropertyMap;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.service.delta.DeltaConsts.DomainMetadataNames;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.ColumnUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hibernate.Session;

/**
 * Translates a Delta {@link UpdateTableRequest} into in-memory mutations on a {@link TableInfoDAO}
 * and {@link MutablePropertyMap}. Three phases, each separately callable:
 *
 * <ol>
 *   <li>{@link #collectRequest} -- pre-transaction shape checks; classify by subtype.
 *   <li>{@link #checkRequirements} -- {@code assert-*} requirements against the loaded DAO.
 *   <li>{@link #applyUpdates} -- dispatch each action onto the DAO and property map.
 * </ol>
 *
 * <p>The mapper does no DB I/O; the caller ({@link io.unitycatalog.server.persist.TableRepository})
 * loads state once and flushes the diff.
 */
public final class DeltaUpdateTableMapper {

  private DeltaUpdateTableMapper() {}

  // ---------------------------------------------------------------------- collection

  public record CollectedRequest(
      CollectedRequirements requirements, CollectedUpdates updates) {}

  /**
   * Classify and shape-check the request. {@code assert-table-uuid} is mandatory: without it a
   * client with a cached three-part name could silently commit to a freshly-recreated table.
   */
  public static CollectedRequest collectRequest(UpdateTableRequest request) {
    if (request == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Request body is required.");
    }
    List<TableUpdate> updates = request.getUpdates();
    if (updates == null || updates.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "At least one update is required.");
    }
    CollectedRequirements requirements = collectRequirements(request.getRequirements());
    if (requirements.assertTableUuid.isEmpty()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "assert-table-uuid requirement is required.");
    }
    CollectedUpdates collected = collectUpdates(updates);
    checkUpdateOverlaps(collected);
    return new CollectedRequest(requirements, collected);
  }

  /**
   * Reject requests that touch the same property key (or domain name) from both a set and a remove
   * action. The mapper's canonical apply order would resolve such an overlap deterministically
   * (remove runs after set), but the intent is contradictory -- almost always a client bug -- so
   * fail fast at shape-check time rather than silently dropping one of the two.
   */
  private static void checkUpdateOverlaps(CollectedUpdates u) {
    if (u.setProperties.isPresent() && u.removeProperties.isPresent()) {
      Map<String, String> setMap = u.setProperties.get().getUpdates();
      List<String> removeList = u.removeProperties.get().getRemovals();
      Set<String> overlap = intersect(setMap == null ? null : setMap.keySet(), removeList);
      if (!overlap.isEmpty()) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "set-properties and remove-properties overlap on keys: " + overlap);
      }
    }
    if (u.setDomainMetadata.isPresent() && u.removeDomainMetadata.isPresent()) {
      Set<String> setDomains = domainsSetIn(u.setDomainMetadata.get().getUpdates());
      List<String> removeList = u.removeDomainMetadata.get().getDomains();
      Set<String> overlap = intersect(setDomains, removeList);
      if (!overlap.isEmpty()) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "set-domain-metadata and remove-domain-metadata overlap on domains: " + overlap);
      }
    }
  }

  /** Domain names with a non-null entry in {@code updates}. */
  private static Set<String> domainsSetIn(DomainMetadataUpdates updates) {
    if (updates == null) {
      return Set.of();
    }
    Set<String> domains = new HashSet<>();
    if (updates.getDeltaClustering() != null) {
      domains.add(DomainMetadataNames.CLUSTERING);
    }
    if (updates.getDeltaRowTracking() != null) {
      domains.add(DomainMetadataNames.ROW_TRACKING);
    }
    return domains;
  }

  /** Set-intersection that tolerates null inputs and yields a sorted view for stable messages. */
  private static Set<String> intersect(Set<String> a, List<String> b) {
    if (a == null || a.isEmpty() || b == null || b.isEmpty()) {
      return Set.of();
    }
    Set<String> out = new TreeSet<>(a);
    out.retainAll(b);
    return out;
  }

  /** One slot per requirement subtype; enforces at-most-one per request. */
  public static final class CollectedRequirements {
    private Optional<AssertTableUUID> assertTableUuid = Optional.empty();
    private Optional<AssertEtag> assertEtag = Optional.empty();

    void putOnce(TableRequirement req) {
      if (req instanceof AssertTableUUID u) {
        assertTableUuid = fillOnce(assertTableUuid, u, TableRequirement::getType);
      } else if (req instanceof AssertEtag e) {
        assertEtag = fillOnce(assertEtag, e, TableRequirement::getType);
      } else {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Unknown requirement type: " + (req == null ? "null" : req.getType()));
      }
    }
  }

  /** One slot per update subtype; enforces at-most-one per request. */
  public static final class CollectedUpdates {
    private Optional<SetPropertiesUpdate> setProperties = Optional.empty();
    private Optional<RemovePropertiesUpdate> removeProperties = Optional.empty();
    private Optional<SetProtocolUpdate> setProtocol = Optional.empty();
    private Optional<SetSchemaUpdate> setSchema = Optional.empty();
    private Optional<SetPartitionColumnsUpdate> setPartitionColumns = Optional.empty();
    private Optional<SetTableCommentUpdate> setTableComment = Optional.empty();
    private Optional<SetDomainMetadataUpdate> setDomainMetadata = Optional.empty();
    private Optional<RemoveDomainMetadataUpdate> removeDomainMetadata = Optional.empty();
    private Optional<UpdateSnapshotVersionUpdate> updateSnapshotVersion = Optional.empty();
    private Optional<AddCommitUpdate> addCommit = Optional.empty();
    private Optional<SetLatestBackfilledVersionUpdate> setLatestBackfilledVersion =
        Optional.empty();

    /**
     * True if this request carries at least one MANAGED-applicable metadata-changing action
     * (properties, protocol, schema, partition columns, comment, domain metadata). Used to gate
     * the auto-stamping of {@code delta.lastUpdateVersion} / {@code delta.lastCommitTimestamp} on
     * a MANAGED {@code add-commit}. {@code update-metadata-snapshot-version} is intentionally
     * omitted: it is EXTERNAL-only and so can never co-occur with {@code add-commit}.
     */
    boolean hasManagedTableMetadataChange() {
      return setProperties.isPresent()
          || removeProperties.isPresent()
          || setProtocol.isPresent()
          || setSchema.isPresent()
          || setPartitionColumns.isPresent()
          || setTableComment.isPresent()
          || setDomainMetadata.isPresent()
          || removeDomainMetadata.isPresent();
    }

    void putOnce(TableUpdate update) {
      if (update instanceof SetPropertiesUpdate u) {
        setProperties = fillOnce(setProperties, u, TableUpdate::getAction);
      } else if (update instanceof RemovePropertiesUpdate u) {
        removeProperties = fillOnce(removeProperties, u, TableUpdate::getAction);
      } else if (update instanceof SetProtocolUpdate u) {
        setProtocol = fillOnce(setProtocol, u, TableUpdate::getAction);
      } else if (update instanceof SetSchemaUpdate u) {
        setSchema = fillOnce(setSchema, u, TableUpdate::getAction);
      } else if (update instanceof SetPartitionColumnsUpdate u) {
        setPartitionColumns = fillOnce(setPartitionColumns, u, TableUpdate::getAction);
      } else if (update instanceof SetTableCommentUpdate u) {
        setTableComment = fillOnce(setTableComment, u, TableUpdate::getAction);
      } else if (update instanceof SetDomainMetadataUpdate u) {
        setDomainMetadata = fillOnce(setDomainMetadata, u, TableUpdate::getAction);
      } else if (update instanceof RemoveDomainMetadataUpdate u) {
        removeDomainMetadata = fillOnce(removeDomainMetadata, u, TableUpdate::getAction);
      } else if (update instanceof UpdateSnapshotVersionUpdate u) {
        updateSnapshotVersion = fillOnce(updateSnapshotVersion, u, TableUpdate::getAction);
      } else if (update instanceof AddCommitUpdate u) {
        addCommit = fillOnce(addCommit, u, TableUpdate::getAction);
      } else if (update instanceof SetLatestBackfilledVersionUpdate u) {
        setLatestBackfilledVersion =
            fillOnce(setLatestBackfilledVersion, u, TableUpdate::getAction);
      } else {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Unknown update action: " + (update == null ? "null" : update.getAction()));
      }
    }
  }

  private static CollectedRequirements collectRequirements(List<TableRequirement> requirements) {
    CollectedRequirements c = new CollectedRequirements();
    if (requirements != null) {
      requirements.forEach(c::putOnce);
    }
    return c;
  }

  private static CollectedUpdates collectUpdates(List<TableUpdate> updates) {
    CollectedUpdates c = new CollectedUpdates();
    updates.forEach(c::putOnce);
    return c;
  }

  /** One-shot slot fill; {@code nameFn} extracts the action/type name for the error message. */
  private static <T> Optional<T> fillOnce(Optional<T> slot, T value, Function<T, String> nameFn) {
    if (slot.isPresent()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "At most one " + nameFn.apply(value) + " is allowed per request.");
    }
    return Optional.of(value);
  }

  // ---------------------------------------------------------------------- requirements check

  /** Failures raise {@link ErrorCode#UPDATE_REQUIREMENT_CONFLICT} so the client retries. */
  public static void checkRequirements(TableInfoDAO dao, CollectedRequest collected) {
    CollectedRequirements r = collected.requirements();
    r.assertTableUuid.ifPresent(u -> checkAssertTableUuid(dao, u));
    r.assertEtag.ifPresent(e -> checkAssertEtag(dao, e));
  }

  private static void checkAssertTableUuid(TableInfoDAO dao, AssertTableUUID u) {
    if (!Objects.equals(u.getUuid(), dao.getId())) {
      throw new BaseException(
          ErrorCode.UPDATE_REQUIREMENT_CONFLICT,
          "assert-table-uuid failed: expected " + u.getUuid() + " but table has " + dao.getId());
    }
  }

  private static void checkAssertEtag(TableInfoDAO dao, AssertEtag e) {
    String currentEtag = computeEtag(dao);
    if (!Objects.equals(currentEtag, e.getEtag())) {
      throw new BaseException(
          ErrorCode.UPDATE_REQUIREMENT_CONFLICT,
          "assert-etag failed: expected " + e.getEtag() + " but table has " + currentEtag);
    }
  }

  /**
   * Shared by the {@code loadTable} response builder and the {@code assert-etag} check.
   *
   * <p>Known weakness: ms-precision {@code updated_at}. If a state-changing update lands in the
   * same wall-clock millisecond as the client's prior read, the etag doesn't advance and a
   * follow-up {@code assert-etag} passes against state the client never observed. The Delta
   * update path holds {@code PESSIMISTIC_WRITE} on the row, so two concurrent {@code
   * updateTableForDelta} calls cannot both pass {@code assert-etag} against the same stale
   * snapshot. {@code assert-etag} is an optional client-side optimization anyway; the
   * authoritative serialization for CCv2 commits is the version conflict check in the commit
   * endpoint.
   */
  public static String computeEtag(TableInfoDAO dao) {
    return dao.getUpdatedAt() != null
        ? "etag-" + dao.getUpdatedAt().getTime()
        : "etag-" + dao.getId();
  }

  // ---------------------------------------------------------------------- apply updates

  /**
   * What the commit-log actions ({@code add-commit}, {@code set-latest-backfilled-version}) prepare
   * for the caller to dispatch via {@code DeltaCommitRepository.applyCommitAndBackfillInSession}.
   * Kept as a value type so the mapper stays free of the commit-repo dependency.
   */
  public record CommitDispatch(
      Optional<DeltaCommit> commit,
      Optional<DeltaUniformUtils.UniformIcebergFields> uniformFields,
      Optional<Long> latestBackfilledVersion) {}

  /**
   * Apply the updates to the DAO and property map, returning the commit-log dispatch (if any) for
   * the caller to forward to {@code DeltaCommitRepository}.
   *
   * <p>Actions run in canonical order (not request order): schema + partition columns first (one
   * combined pass via {@link #applySchemaAndPartitionColumns} when both are present); then protocol
   * / properties / domain-metadata / comment / snapshot version so the UniForm-presence check sees
   * the post-update property map; finally the commit + backfill pair is shaped into a {@link
   * CommitDispatch}.
   */
  public static Optional<CommitDispatch> applyUpdates(
      Session session,
      TableInfoDAO dao,
      MutablePropertyMap properties,
      CollectedRequest collected) {
    CollectedUpdates c = collected.updates();
    applySchemaAndPartitionColumns(session, dao, c.setSchema, c.setPartitionColumns);
    c.setProtocol.ifPresent(u -> applySetProtocol(properties, u.getProtocol()));
    c.setProperties.ifPresent(u -> applySetProperties(properties, u.getUpdates()));
    c.removeProperties.ifPresent(u -> applyRemoveProperties(properties, u.getRemovals()));
    c.setDomainMetadata.ifPresent(u -> applySetDomainMetadata(properties, u.getUpdates()));
    c.removeDomainMetadata.ifPresent(u -> applyRemoveDomainMetadata(properties, u.getDomains()));
    c.setTableComment.ifPresent(u -> applySetTableComment(dao, u));
    c.updateSnapshotVersion.ifPresent(u -> applyUpdateSnapshotVersion(dao, properties, u));
    // Re-validate the MANAGED contract against the final post-apply state. set-protocol runs
    // full validation; set-domain-metadata alone only needs the DM-vs-writer-features check.
    if (TableType.MANAGED.toString().equals(dao.getType())) {
      DomainMetadataUpdates effectiveDm =
          DeltaPropertyMapper.synthesizeDomainMetadataFromProperties(properties.asMap());
      if (c.setProtocol.isPresent()) {
        UcManagedDeltaContract.validate(
            c.setProtocol.get().getProtocol(), effectiveDm, properties.asMap());
      } else if (c.setDomainMetadata.isPresent()) {
        UcManagedDeltaContract.validateDomainMetadataAgainstWriterFeatures(
            DeltaPropertyMapper.extractFeaturesFromProperties(properties.asMap()), effectiveDm);
      }
    }
    if (c.addCommit.isPresent() || c.setLatestBackfilledVersion.isPresent()) {
      return Optional.of(
          prepareCommitAndBackfill(
              dao,
              properties,
              c.addCommit,
              c.setLatestBackfilledVersion,
              c.hasManagedTableMetadataChange()));
    }
    return Optional.empty();
  }

  private static void applySetProperties(MutablePropertyMap properties, Map<String, String> toSet) {
    if (toSet == null || toSet.isEmpty()) {
      return;
    }
    properties.putAll(toSet);
  }

  private static void applyRemoveProperties(MutablePropertyMap properties, List<String> toRemove) {
    if (toRemove == null || toRemove.isEmpty()) {
      return;
    }
    properties.removeAll(toRemove);
  }

  /** Full replacement of the protocol block; other stored properties are left alone. */
  private static void applySetProtocol(MutablePropertyMap properties, DeltaProtocol protocol) {
    ValidationUtils.checkNotNull(protocol, "set-protocol requires a protocol.");
    properties.removeMatchingPrefix(TableProperties.FEATURE_PREFIX);
    Map<String, String> derived = new HashMap<>();
    DeltaPropertyMapper.deriveFromProtocol(derived, protocol);
    properties.putAll(derived);
  }

  /**
   * Apply schema and/or partition-column changes by building the post-update column list once and
   * swapping the DAO column collection. The spec frames {@code set-columns} and {@code
   * set-partition-columns} as independent actions; the absent action's concern is preserved from
   * the existing DAO so a column-only request can't silently desync the partition list (and vice
   * versa). If a partition column is missing from the resulting schema, the request is rejected
   * with {@code partition-columns references unknown column: ...}.
   *
   * <p>Swap semantics rely on the orphanRemoval mapping on {@link TableInfoDAO#getColumns()} to
   * clean up the old rows; the intervening flush ensures the deletes hit the DB before the
   * inserts, so the {@code (table_id, ordinal_position, name)} unique constraint doesn't trip.
   */
  private static void applySchemaAndPartitionColumns(
      Session session,
      TableInfoDAO dao,
      Optional<SetSchemaUpdate> setSchema,
      Optional<SetPartitionColumnsUpdate> setPartition) {
    if (setSchema.isEmpty() && setPartition.isEmpty()) {
      return;
    }
    // Source of the new schema: the request when set-columns is present, otherwise the existing
    // DAO with partition indices cleared so applyPartitionColumns can re-stamp them below.
    List<ColumnInfo> newColumns;
    if (setSchema.isPresent()) {
      StructType columns =
          ValidationUtils.checkNotNull(
              setSchema.get().getColumns(), "set-columns requires a columns block.");
      List<StructField> fields =
          ValidationUtils.checkNotNull(
              columns.getFields(), "set-columns requires columns.fields.");
      if (fields.isEmpty()) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT, "set-columns requires at least one column.");
      }
      newColumns = ColumnUtils.toColumnInfos(fields);
    } else {
      newColumns = ColumnInfoDAO.toList(dao.getColumns());
      newColumns.forEach(c -> c.setPartitionIndex(null));
    }
    // Source of the partition list: the request when set-partition-columns is present, otherwise
    // the existing DAO's partition columns (preserved by name across a column-only action).
    List<String> partitionNames;
    if (setPartition.isPresent()) {
      partitionNames =
          ValidationUtils.checkNotNull(
              setPartition.get().getPartitionColumns(),
              "set-partition-columns requires a partition-columns list.");
    } else {
      partitionNames = currentPartitionColumnNames(dao);
    }
    ColumnUtils.applyPartitionColumns(newColumns, partitionNames);
    List<ColumnInfoDAO> newColumnDAOs = ColumnInfoDAO.fromList(newColumns);
    newColumnDAOs.forEach(
        c -> {
          c.setId(UUID.randomUUID());
          c.setTable(dao);
        });
    dao.getColumns().clear();
    session.flush();
    dao.getColumns().addAll(newColumnDAOs);
  }

  /** Names of the DAO's partition columns, ordered by partition index. */
  private static List<String> currentPartitionColumnNames(TableInfoDAO dao) {
    return ColumnInfoDAO.toList(dao.getColumns()).stream()
        .filter(c -> c.getPartitionIndex() != null)
        .sorted(Comparator.comparingInt(ColumnInfo::getPartitionIndex))
        .map(ColumnInfo::getName)
        .collect(Collectors.toList());
  }

  private static void applySetDomainMetadata(
      MutablePropertyMap properties, DomainMetadataUpdates updates) {
    ValidationUtils.checkNotNull(updates, "set-domain-metadata requires an updates block.");
    Map<String, String> derived = new HashMap<>();
    DeltaPropertyMapper.deriveFromDomainMetadata(derived, updates);
    if (derived.isEmpty()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "set-domain-metadata requires at least one domain entry. "
              + "Supported domains: "
              + new TreeSet<>(DeltaPropertyMapper.DOMAIN_TO_PROPERTY_KEY.keySet()));
    }
    properties.putAll(derived);
  }

  private static void applyRemoveDomainMetadata(
      MutablePropertyMap properties, List<String> domains) {
    if (domains == null || domains.isEmpty()) {
      return;
    }
    for (String domain : domains) {
      String propertyKey = DeltaPropertyMapper.DOMAIN_TO_PROPERTY_KEY.get(domain);
      if (propertyKey == null) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT, "Unknown domain in remove-domain-metadata: " + domain);
      }
      properties.remove(propertyKey);
    }
  }

  private static void applySetTableComment(TableInfoDAO dao, SetTableCommentUpdate update) {
    ValidationUtils.checkNotNull(update.getComment(), "set-table-comment requires a comment.");
    dao.setComment(update.getComment());
  }

  private static void applyUpdateSnapshotVersion(
      TableInfoDAO dao, MutablePropertyMap properties, UpdateSnapshotVersionUpdate update) {
    if (!TableType.EXTERNAL.toString().equals(dao.getType())) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "update-metadata-snapshot-version is only supported for EXTERNAL Delta tables; "
              + "for MANAGED tables, use the Delta commit endpoint.");
    }
    ValidationUtils.checkNotNull(update.getLastCommitVersion(),
      "update-metadata-snapshot-version requires last-commit-version.");
    ValidationUtils.checkNotNull(update.getLastCommitTimestampMs(),
      "update-metadata-snapshot-version requires last-commit-timestamp-ms.");
    properties.put(
        TableProperties.LAST_UPDATE_VERSION, String.valueOf(update.getLastCommitVersion()));
    properties.put(
        TableProperties.LAST_COMMIT_TIMESTAMP, String.valueOf(update.getLastCommitTimestampMs()));
  }

  // ---------------------------------------------------------------------- commit + backfill

  /**
   * Mapper-side preparation for the {@code add-commit} and {@code set-latest-backfilled-version}
   * actions: cross-action validation plus snapshot-property bookkeeping. Returns the
   * {@link CommitDispatch} the caller forwards to {@code
   * DeltaCommitRepository.applyCommitAndBackfillInSession} so the mapper stays free of the
   * commit-repo dependency.
   *
   * <ul>
   *   <li>require MANAGED -- both actions are only legal on UC catalog-managed Delta tables (DELTA
   *       format is guaranteed by the caller's prior {@code requireDeltaTable});
   *   <li>extract and shape-validate uniform fields when {@code add-commit} carries them;
   *   <li>pin uniform.iceberg.converted-delta-version equal to commit.version (the commit-time
   *       check that the create-time uniform validator can't run because there is no commit
   *       version at create);
   *   <li>run the UniForm presence-consistency check against the post-update property map;
   *   <li>stamp {@code delta.lastUpdateVersion} / {@code delta.lastCommitTimestamp} when the
   *       commit is metadata-changing;
   *   <li>read {@code latest-published-version} off the backfill action and reject if it's null.
   * </ul>
   */
  private static CommitDispatch prepareCommitAndBackfill(
      TableInfoDAO dao,
      MutablePropertyMap properties,
      Optional<AddCommitUpdate> addCommitOpt,
      Optional<SetLatestBackfilledVersionUpdate> backfillOpt,
      boolean hasManagedTableMetadataChange) {
    requireManaged(dao);
    Optional<DeltaUniformUtils.UniformIcebergFields> uniformFields = Optional.empty();
    Optional<DeltaCommit> commitOpt = Optional.empty();
    if (addCommitOpt.isPresent()) {
      AddCommitUpdate addCommit = addCommitOpt.get();
      DeltaCommit commit =
          ValidationUtils.checkNotNull(
              addCommit.getCommit(), "add-commit requires a commit block.");
      commitOpt = Optional.of(commit);
      uniformFields = DeltaUniformUtils.getUniformFields(addCommit.getUniform());
      uniformFields.ifPresent(
          f -> DeltaUniformUtils.requireConvertedDeltaVersionEquals(f, commit.getVersion()));
      DeltaUniformUtils.validateConsistency(properties.asMap(), uniformFields.isPresent());
      // For MANAGED tables, a metadata-changing add-commit advances the snapshot bookkeeping
      // properties. UC is the authoritative commit coordinator, so the client doesn't need to
      // send them separately.
      if (hasManagedTableMetadataChange) {
        properties.put(TableProperties.LAST_UPDATE_VERSION, String.valueOf(commit.getVersion()));
        properties.put(
            TableProperties.LAST_COMMIT_TIMESTAMP, String.valueOf(commit.getTimestamp()));
      }
    }
    // The Delta wire field is named `latest-published-version`; the UC repo's parameter is
    // `latestBackfilledVersion` (same value, different perspective). Keep the local aligned with
    // the repo so the call site reads cleanly.
    Optional<Long> latestBackfilledVersion =
        backfillOpt.map(
            b ->
                ValidationUtils.checkNotNull(
                    b.getLatestPublishedVersion(),
                    "set-latest-backfilled-version requires latest-published-version."));
    return new CommitDispatch(commitOpt, uniformFields, latestBackfilledVersion);
  }

  /**
   * Reject non-MANAGED tables at the commit/backfill entry. DELTA format is guaranteed by the
   * caller's prior {@code requireDeltaTable}, so we only check the type here.
   */
  private static void requireManaged(TableInfoDAO dao) {
    if (!TableType.MANAGED.toString().equals(dao.getType())) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "add-commit and set-latest-backfilled-version require a MANAGED Delta table.");
    }
  }
}
