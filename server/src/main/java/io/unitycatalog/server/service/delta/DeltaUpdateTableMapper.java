package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.delta.model.DeltaAddCommitUpdate;
import io.unitycatalog.server.delta.model.DeltaArrayType;
import io.unitycatalog.server.delta.model.DeltaAssertEtag;
import io.unitycatalog.server.delta.model.DeltaAssertTableUUID;
import io.unitycatalog.server.delta.model.DeltaCommit;
import io.unitycatalog.server.delta.model.DeltaDataType;
import io.unitycatalog.server.delta.model.DeltaDomainMetadataUpdates;
import io.unitycatalog.server.delta.model.DeltaMapType;
import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.DeltaRemoveDomainMetadataUpdate;
import io.unitycatalog.server.delta.model.DeltaRemovePropertiesUpdate;
import io.unitycatalog.server.delta.model.DeltaSetDomainMetadataUpdate;
import io.unitycatalog.server.delta.model.DeltaSetLatestBackfilledVersionUpdate;
import io.unitycatalog.server.delta.model.DeltaSetPartitionColumnsUpdate;
import io.unitycatalog.server.delta.model.DeltaSetPropertiesUpdate;
import io.unitycatalog.server.delta.model.DeltaSetProtocolUpdate;
import io.unitycatalog.server.delta.model.DeltaSetSchemaUpdate;
import io.unitycatalog.server.delta.model.DeltaSetTableCommentUpdate;
import io.unitycatalog.server.delta.model.DeltaStructField;
import io.unitycatalog.server.delta.model.DeltaStructFieldMetadata;
import io.unitycatalog.server.delta.model.DeltaStructType;
import io.unitycatalog.server.delta.model.DeltaTableRequirement;
import io.unitycatalog.server.delta.model.DeltaTableUpdate;
import io.unitycatalog.server.delta.model.DeltaUpdateSnapshotVersionUpdate;
import io.unitycatalog.server.delta.model.DeltaUpdateTableRequest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.DeltaCommitInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.MutablePropertyMap;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.service.delta.DeltaConsts.DomainMetadataNames;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.ColumnUtils;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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
 * Translates a {@link DeltaUpdateTableRequest} into in-memory mutations on a {@link TableInfoDAO}
 * and {@link MutablePropertyMap}. Three phases, each separately callable:
 *
 * <ol>
 *   <li>{@link #collectRequest} -- pre-transaction shape checks; classify by subtype.
 *   <li>{@link #checkTableUuidRequirement} / {@link #checkEtagRequirement} -- {@code assert-*}
 *       requirements against the loaded DAO.
 *   <li>{@link #applyUpdates} -- dispatch each action onto the DAO and property map.
 * </ol>
 *
 * <p>The mapper does no DB I/O; the caller ({@link io.unitycatalog.server.persist.TableRepository})
 * loads state once and flushes the diff.
 */
public final class DeltaUpdateTableMapper {

  private DeltaUpdateTableMapper() {}

  // ---------------------------------------------------------------------- collection

  public record CollectedRequest(CollectedRequirements requirements, CollectedUpdates updates) {}

  /**
   * Classify and shape-check the request. {@code assert-table-uuid} is mandatory: without it a
   * client with a cached three-part name could silently commit to a freshly-recreated table.
   */
  public static CollectedRequest collectRequest(DeltaUpdateTableRequest request) {
    if (request == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Request body is required.");
    }
    List<DeltaTableUpdate> updates = request.getUpdates();
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
  private static Set<String> domainsSetIn(DeltaDomainMetadataUpdates updates) {
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
    private Optional<DeltaAssertTableUUID> assertTableUuid = Optional.empty();
    private Optional<DeltaAssertEtag> assertEtag = Optional.empty();

    void putOnce(DeltaTableRequirement req) {
      if (req instanceof DeltaAssertTableUUID u) {
        assertTableUuid = fillOnce(assertTableUuid, u, DeltaTableRequirement::getType);
      } else if (req instanceof DeltaAssertEtag e) {
        assertEtag = fillOnce(assertEtag, e, DeltaTableRequirement::getType);
      } else {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Unknown requirement type: " + (req == null ? "null" : req.getType()));
      }
    }
  }

  /** One slot per update subtype; enforces at-most-one per request. */
  public static final class CollectedUpdates {
    private Optional<DeltaSetPropertiesUpdate> setProperties = Optional.empty();
    private Optional<DeltaRemovePropertiesUpdate> removeProperties = Optional.empty();
    private Optional<DeltaSetProtocolUpdate> setProtocol = Optional.empty();
    private Optional<DeltaSetSchemaUpdate> setSchema = Optional.empty();
    private Optional<DeltaSetPartitionColumnsUpdate> setPartitionColumns = Optional.empty();
    private Optional<DeltaSetTableCommentUpdate> setTableComment = Optional.empty();
    private Optional<DeltaSetDomainMetadataUpdate> setDomainMetadata = Optional.empty();
    private Optional<DeltaRemoveDomainMetadataUpdate> removeDomainMetadata = Optional.empty();
    private Optional<DeltaUpdateSnapshotVersionUpdate> updateSnapshotVersion = Optional.empty();
    private Optional<DeltaAddCommitUpdate> addCommit = Optional.empty();
    private Optional<DeltaSetLatestBackfilledVersionUpdate> setLatestBackfilledVersion =
        Optional.empty();

    /**
     * True if this request carries at least one MANAGED-applicable metadata-changing action
     * (properties, protocol, schema, partition columns, comment, domain metadata). Used to gate the
     * auto-stamping of {@code delta.lastUpdateVersion} / {@code delta.lastCommitTimestamp} on a
     * MANAGED {@code add-commit}. {@code update-metadata-snapshot-version} is intentionally
     * omitted: it is EXTERNAL-only and so can never co-occur with {@code add-commit}.
     *
     * <p>A {@code set-domain-metadata} that touches only the {@code delta.rowTracking} domain does
     * not count: row-tracking high-water-mark updates are per-commit snapshot bookkeeping, not
     * metadata changes. The Delta protocol requires writers to mirror the high-water mark in every
     * commit that assigns fresh row IDs, so clients send it alongside otherwise data-only {@code
     * add-commit}s. Counting it as a metadata change would incorrectly advance {@code updatedAt}
     * and the {@code delta.lastUpdateVersion} stamp on every data commit of a row-tracking table.
     * The high-water-mark property itself is still persisted.
     */
    boolean hasManagedTableMetadataChange() {
      return setProperties.isPresent()
          || removeProperties.isPresent()
          || setProtocol.isPresent()
          || setSchema.isPresent()
          || setPartitionColumns.isPresent()
          || setTableComment.isPresent()
          || setsDomainMetadataBeyondRowTracking()
          || removeDomainMetadata.isPresent();
    }

    /**
     * True if {@code set-domain-metadata} is present and sets any domain other than {@code
     * delta.rowTracking}. Extend this check when new domains are added to {@link
     * DeltaDomainMetadataUpdates}.
     */
    private boolean setsDomainMetadataBeyondRowTracking() {
      if (setDomainMetadata.isEmpty()) {
        return false;
      }
      DeltaDomainMetadataUpdates updates = setDomainMetadata.get().getUpdates();
      return updates != null && updates.getDeltaClustering() != null;
    }

    /**
     * True if this request changes catalog-visible table metadata: any MANAGED-applicable metadata
     * action ({@link #hasManagedTableMetadataChange()}) or the EXTERNAL-only {@code
     * update-metadata-snapshot-version}. Data-only {@code add-commit} and backfill-only requests
     * carry no metadata change and return false.
     */
    public boolean changesTableMetadata() {
      return hasManagedTableMetadataChange() || updateSnapshotVersion.isPresent();
    }

    void putOnce(DeltaTableUpdate update) {
      if (update instanceof DeltaSetPropertiesUpdate u) {
        setProperties = fillOnce(setProperties, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaRemovePropertiesUpdate u) {
        removeProperties = fillOnce(removeProperties, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaSetProtocolUpdate u) {
        setProtocol = fillOnce(setProtocol, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaSetSchemaUpdate u) {
        setSchema = fillOnce(setSchema, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaSetPartitionColumnsUpdate u) {
        setPartitionColumns = fillOnce(setPartitionColumns, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaSetTableCommentUpdate u) {
        setTableComment = fillOnce(setTableComment, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaSetDomainMetadataUpdate u) {
        setDomainMetadata = fillOnce(setDomainMetadata, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaRemoveDomainMetadataUpdate u) {
        removeDomainMetadata = fillOnce(removeDomainMetadata, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaUpdateSnapshotVersionUpdate u) {
        updateSnapshotVersion = fillOnce(updateSnapshotVersion, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaAddCommitUpdate u) {
        addCommit = fillOnce(addCommit, u, DeltaTableUpdate::getAction);
      } else if (update instanceof DeltaSetLatestBackfilledVersionUpdate u) {
        setLatestBackfilledVersion =
            fillOnce(setLatestBackfilledVersion, u, DeltaTableUpdate::getAction);
      } else {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Unknown update action: " + (update == null ? "null" : update.getAction()));
      }
    }
  }

  private static CollectedRequirements collectRequirements(
      List<DeltaTableRequirement> requirements) {
    CollectedRequirements c = new CollectedRequirements();
    if (requirements != null) {
      requirements.forEach(c::putOnce);
    }
    return c;
  }

  private static CollectedUpdates collectUpdates(List<DeltaTableUpdate> updates) {
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

  /**
   * Checks the {@code assert-table-uuid} requirement, if present, raising {@link
   * ErrorCode#UPDATE_REQUIREMENT_CONFLICT} on mismatch.
   */
  public static void checkTableUuidRequirement(TableInfoDAO dao, CollectedRequest collected) {
    Optional<UUID> assertUuid =
        collected.requirements().assertTableUuid.map(DeltaAssertTableUUID::getUuid);
    UUID tableUuid = dao.getId();
    if (assertUuid.isPresent() && !Objects.equals(assertUuid.get(), tableUuid)) {
      throw new BaseException(
          ErrorCode.UPDATE_REQUIREMENT_CONFLICT,
          "assert-table-uuid failed: expected " + assertUuid.get() + " but table has " + tableUuid);
    }
  }

  /**
   * Checks the {@code assert-etag} requirement, if present, against {@code preApplyEtag}, raising
   * {@link ErrorCode#UPDATE_REQUIREMENT_CONFLICT} on mismatch. The caller passes the pre-apply etag
   * because applying a commit can advance {@code updatedAt} (and the etag), so recomputing here
   * would compare against post-mutation state.
   */
  public static void checkEtagRequirement(String preApplyEtag, CollectedRequest collected) {
    Optional<String> assertEtag = collected.requirements().assertEtag.map(DeltaAssertEtag::getEtag);
    if (assertEtag.isPresent() && !Objects.equals(preApplyEtag, assertEtag.get())) {
      throw new BaseException(
          ErrorCode.UPDATE_REQUIREMENT_CONFLICT,
          "assert-etag failed: expected " + assertEtag.get() + " but table has " + preApplyEtag);
    }
  }

  /**
   * Shared by the {@code loadTable} response builder and the {@code assert-etag} check.
   *
   * <p>Known weakness: ms-precision {@code updated_at}. If a state-changing update lands in the
   * same wall-clock millisecond as the client's prior read, the etag doesn't advance and a
   * follow-up {@code assert-etag} passes against state the client never observed. The Delta update
   * path holds {@code PESSIMISTIC_WRITE} on the row, so two concurrent {@code updateTableForDelta}
   * calls cannot both pass {@code assert-etag} against the same stale snapshot. {@code assert-etag}
   * is an optional client-side optimization anyway; the authoritative serialization for CCv2
   * commits is the version conflict check in the commit endpoint.
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
      Optional<DeltaCommitInfo> commit,
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
      CollectedRequest collected,
      ServerProperties serverProperties) {
    CollectedUpdates c = collected.updates();
    String rawMode = properties.get(DeltaConsts.TableProperties.COLUMN_MAPPING_MODE);
    String columnMappingMode = normalizeColumnMappingMode(rawMode);
    applySchemaAndPartitionColumns(
        session, dao, c.setSchema, c.setPartitionColumns, columnMappingMode);
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
      DeltaDomainMetadataUpdates effectiveDm =
          DeltaPropertyMapper.synthesizeDomainMetadataFromProperties(properties.asMap());
      if (c.setProtocol.isPresent()) {
        UcManagedDeltaContract.validate(
            c.setProtocol.get().getProtocol(), effectiveDm, properties.asMap(), serverProperties);
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
   * clean up the old rows; the intervening flush ensures the deletes hit the DB before the inserts,
   * so the {@code (table_id, ordinal_position, name)} unique constraint doesn't trip.
   *
   * <p>On every {@code set-columns}, each column receives a fresh database UUID (to match standard
   * catalog semantics where column identity is refreshed on every set-columns). All hard-validation
   * guards for column-mapping rename/drop/reassign are enforced via {@link
   * #validateColumnRenameAndDropGuards} before the swap runs.
   */
  private static void applySchemaAndPartitionColumns(
      Session session,
      TableInfoDAO dao,
      Optional<DeltaSetSchemaUpdate> setSchema,
      Optional<DeltaSetPartitionColumnsUpdate> setPartition,
      String columnMappingMode) {
    if (setSchema.isEmpty() && setPartition.isEmpty()) {
      return;
    }
    // Source of the new schema: the request when set-columns is present, otherwise the existing
    // DAO with partition indices cleared so applyPartitionColumns can re-stamp them below.
    List<ColumnInfo> newColumns;
    if (setSchema.isPresent()) {
      DeltaStructType columns =
          ValidationUtils.checkNotNull(
              setSchema.get().getColumns(), "set-columns requires a columns block.");
      List<DeltaStructField> fields =
          ValidationUtils.checkNotNull(columns.getFields(), "set-columns requires columns.fields.");
      if (fields.isEmpty()) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT, "set-columns requires at least one column.");
      }
      ColumnUtils.validateStructType(columns, "columns");
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
    // Hard-validation guards for column-mapping rename / drop / value-reassignment.
    // Must run before validateNoCmIdentityStripped since the guards short-circuit early on
    // no-CM and first-enable paths before the identity-strip check is reached.
    if (setSchema.isPresent()) {
      validateColumnRenameAndDropGuards(dao.getColumns(), newColumns, columnMappingMode);
      // Reject any retained column (same logical name) that lost its active mode CM key.
      // Mode-specific and recursive into nested types.
      validateNoCmIdentityStripped(dao.getColumns(), newColumns, columnMappingMode);
    }
    // Build the new DAO list. On every set-columns, every column receives a fresh UUID
    // (standard catalog semantics; the UUID has no known OSS consumers keying on it).
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

  // ---------------------------------------------------------------------- column-mapping guards

  /**
   * Enforces all hard-validation guards for column-mapping correctness on a {@code set-columns}
   * request, checked in order of execution:
   *
   * <ul>
   *   <li>Reject a duplicate {@code delta.columnMapping.id} in the incoming schema (id-mode only,
   *       globally across all nesting levels).
   *   <li>Reject the incoming schema dropping all column-mapping metadata when the current schema
   *       had it.
   *   <li>Reject any column name removal without column-mapping metadata (would cause silent data
   *       loss).
   *   <li>For first-enable (adding CM where none existed): reject any incoming column that omits
   *       {@code delta.columnMapping.physicalName}.
   *   <li>For first-enable: reject any rename in the same request (renames must be separate after
   *       CM enablement).
   *   <li>Reject more than one top-level column rename per request.
   *   <li>Reject a same-name rename (case-insensitive). This is a defensive check; case-only
   *       changes are filtered during detection.
   *   <li>Reject a retained column whose column-mapping key value changed (id-mode: {@code
   *       delta.columnMapping.id}; name-mode: {@code delta.columnMapping.physicalName}).
   *   <li>Reject stripping the active mode's CM identity key from a retained column (handled via
   *       {@link #validateNoCmIdentityStripped}).
   * </ul>
   */
  private static void validateColumnRenameAndDropGuards(
      List<ColumnInfoDAO> existingDaos,
      List<ColumnInfo> incomingColumns,
      String columnMappingMode) {
    List<ColumnInfo> existingColumns = ColumnInfoDAO.toList(existingDaos);

    // Reject duplicate delta.columnMapping.id in the incoming schema (id-mode only -- ids must be
    // globally unique across all nesting levels; physicalName duplicates are not checked here).
    detectDuplicateCmId(incomingColumns)
        .ifPresent(
            dupId -> {
              throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT,
                  "Duplicate column mapping id '" + dupId + "' in incoming schema.");
            });

    // In name-mode, top-level physicalNames must be unique (physical names are sibling-unique). A
    // duplicate would be silently collapsed by the identity maps that rename detection and
    // partition
    // preservation build, mis-associating a column with the wrong identity, so reject it here.
    if ("name".equals(columnMappingMode)) {
      detectDuplicateTopLevelPhysicalName(incomingColumns)
          .ifPresent(
              dup -> {
                throw new BaseException(
                    ErrorCode.INVALID_ARGUMENT,
                    "Duplicate column mapping physicalName '"
                        + dup
                        + "' on top-level columns in incoming schema; each top-level column must"
                        + " have a unique physical name.");
              });
    }

    // Mode-specific CM presence checks: determine whether the ACTIVE mode's key
    // (delta.columnMapping.id for id-mode, delta.columnMapping.physicalName for name-mode) is
    // present anywhere in the schema (recursively, across all nesting levels). For "none" mode the
    // active key is null and both flags are false, so the code falls through to the
    // rename-without-mapping / first-enable logic below.
    String activeCmKey = activeCmKeyForMode(columnMappingMode);
    boolean currentHasActiveCm =
        activeCmKey != null && hasCmKeyAnywhere(existingColumns, activeCmKey);
    boolean incomingHasActiveCm =
        activeCmKey != null && hasCmKeyAnywhere(incomingColumns, activeCmKey);
    // Mode-agnostic presence: used to distinguish "no CM at all" from "CM present but wrong key"
    // on the first-enable / no-CM paths.
    boolean incomingHasAnyCm = hasAnyCmMetadata(incomingColumns);

    if (!incomingHasActiveCm) {
      if (currentHasActiveCm) {
        // Reject: incoming schema dropped the active mode's CM key while current schema had it.
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "set-columns dropped column mapping metadata. "
                + "Removing column mapping is not supported via the Delta REST catalog.");
      }
      if (!incomingHasAnyCm) {
        // Reject any name removal when neither schema has any CM metadata.
        List<String> removedNames = namesRemovedWithoutMapping(existingColumns, incomingColumns);
        if (!removedNames.isEmpty()) {
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT,
              "Column mapping is required to rename or drop a column; '"
                  + removedNames.get(0)
                  + "' cannot be removed while column mapping is disabled. "
                  + "Enable column mapping first.");
        }
        // No CM anywhere, no names removed: pass through (add / reorder / type-change are safe).
        return;
      }
      // incomingHasAnyCm but not the active mode's key → fall through to first-enable path.
    }

    if (!currentHasActiveCm) {
      // First-enable path: current has no active-mode CM key; incoming introduces CM.

      // Every top-level incoming column must carry physicalName so a hidden rename can be
      // safely ruled out (by convention, physicalName == current logical name on first-enable).
      if (hasAnyMissingPhysicalName(incomingColumns)) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "set-columns: enabling column mapping requires a physical column name "
                + "for every top-level column; this request omits one, so a rename cannot be "
                + "safely ruled out.");
      }

      // First-enable + rename in the same request is not supported. Column-mapping enable and
      // rename must be separate operations, since enabling CM changes the identity semantics and
      // merging both into one commit would degrade the rename to a drop+add at the storage level.
      List<String> feRenames = detectFirstEnableRenames(existingColumns, incomingColumns);
      if (!feRenames.isEmpty()) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "set-columns: renaming a column while enabling column mapping is not supported "
                + "in a single request. Enable column mapping in a separate statement, "
                + "then rename the column.");
      }
      // Plain CM-enable with no rename: pass through.
      return;
    }

    // Normal path: the current schema already has column-mapping metadata.

    // At most one top-level rename per request.
    List<TopLevelRename> topLevelRenames = detectTopLevelRenames(existingColumns, incomingColumns);
    if (topLevelRenames.size() > 1) {
      String renameList =
          topLevelRenames.stream()
              .map(r -> r.oldLogicalName() + "->" + r.newLogicalName())
              .sorted()
              .collect(Collectors.joining(", "));
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Only a single top-level column rename is supported per request; this request renames "
              + topLevelRenames.size()
              + " columns ["
              + renameList
              + "]. Split them into separate ALTER statements.");
    }

    // Defensive guard: reject a "rename" where the new name is case-insensitively equal to the
    // old name. With the equalsIgnoreCase filter inside detectTopLevelRenames this branch is dead
    // code in practice; it remains as a safety net in case detection logic changes.
    for (TopLevelRename rename : topLevelRenames) {
      if (rename.newLogicalName().equalsIgnoreCase(rename.oldLogicalName())) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Column rename to '"
                + rename.newLogicalName()
                + "' is identical to the original column name.");
      }
    }

    // Reassignment guard: a retained column (same logical name in both schemas) must not change
    // its column-mapping key value. The mode (id vs. name) determines which key to check.
    // columnMappingMode is passed in from the table property (delta.columnMapping.mode).
    //
    // A logical name vacated by a rename is excluded: an incoming column reusing that name is a
    // newly added column (rename a -> b plus add a), not a reassignment of the renamed-away column.
    // (A rename that also strips its own column-mapping key is, without the key, indistinguishable
    // from a supported drop+add and is intentionally treated as drop+add, not as a rename.)
    if (!"none".equals(columnMappingMode)) {
      Set<String> renamedAwayLowerNames =
          topLevelRenames.stream()
              .map(r -> r.oldLogicalName().toLowerCase(Locale.ROOT))
              .collect(Collectors.toSet());
      List<String> reassigned =
          detectCmValueReassignment(
              existingColumns, incomingColumns, columnMappingMode, renamedAwayLowerNames);
      if (!reassigned.isEmpty()) {
        reassigned = new ArrayList<>(reassigned);
        reassigned.sort(String::compareTo);
        String keyName = "id".equals(columnMappingMode) ? "id" : "physicalName";
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Column mapping "
                + keyName
                + " reassignment is not supported for retained column(s): "
                + String.join(", ", reassigned));
      }
    }
  }

  /**
   * Returns {@code true} if any column in {@code columns} carries a column-mapping identity key
   * ({@code delta.columnMapping.id} or {@code delta.columnMapping.physicalName}) in its {@code
   * type_json} metadata.
   */
  private static boolean hasAnyCmMetadata(List<ColumnInfo> columns) {
    return columns.stream().anyMatch(c -> mappingKeyOf(c) != null);
  }

  /**
   * Returns the logical names of fields present in {@code current} but absent (by case-insensitive
   * name comparison) from {@code incoming}, recursing into the nested struct/array/map fields of a
   * surviving column. A surviving column whose nested type changes shape (e.g. struct to scalar)
   * drops every field name underneath it, so those names are reported too. A non-empty result
   * signals a rename or drop -- including a nested destructive rewrite -- without column-mapping
   * metadata to track the identity change, which would cause silent data loss in the storage
   * engine.
   */
  private static List<String> namesRemovedWithoutMapping(
      List<ColumnInfo> current, List<ColumnInfo> incoming) {
    List<DeltaStructField> curFields =
        current.stream().map(ColumnUtils::toStructField).collect(Collectors.toList());
    List<DeltaStructField> incFields =
        incoming.stream().map(ColumnUtils::toStructField).collect(Collectors.toList());
    return namesRemovedWithoutMappingInFields(curFields, incFields);
  }

  /** Recursive worker for {@link #namesRemovedWithoutMapping} over sibling field lists. */
  private static List<String> namesRemovedWithoutMappingInFields(
      List<DeltaStructField> curFields, List<DeltaStructField> incFields) {
    Map<String, DeltaStructField> incByLower =
        incFields.stream()
            .collect(
                Collectors.toMap(f -> f.getName().toLowerCase(Locale.ROOT), Function.identity()));
    List<String> removed = new ArrayList<>();
    for (DeltaStructField cur : curFields) {
      DeltaStructField inc = incByLower.get(cur.getName().toLowerCase(Locale.ROOT));
      if (inc == null) {
        removed.add(cur.getName());
      } else {
        removed.addAll(namesRemovedWithoutMappingInTypes(cur.getType(), inc.getType()));
      }
    }
    return removed;
  }

  /**
   * Recurses into matching nested type pairs for {@link #namesRemovedWithoutMapping}. A shape
   * mismatch under a surviving column (its nested type changed kind) reports every current nested
   * field name as removed, since those fields no longer exist.
   */
  private static List<String> namesRemovedWithoutMappingInTypes(
      DeltaDataType curType, DeltaDataType incType) {
    if (curType instanceof DeltaStructType curStruct
        && incType instanceof DeltaStructType incStruct) {
      return namesRemovedWithoutMappingInFields(
          curStruct.getFields() != null ? curStruct.getFields() : List.of(),
          incStruct.getFields() != null ? incStruct.getFields() : List.of());
    } else if (curType instanceof DeltaArrayType curArr
        && incType instanceof DeltaArrayType incArr) {
      return namesRemovedWithoutMappingInTypes(curArr.getElementType(), incArr.getElementType());
    } else if (curType instanceof DeltaMapType curMap && incType instanceof DeltaMapType incMap) {
      List<String> result =
          new ArrayList<>(
              namesRemovedWithoutMappingInTypes(curMap.getKeyType(), incMap.getKeyType()));
      result.addAll(
          namesRemovedWithoutMappingInTypes(curMap.getValueType(), incMap.getValueType()));
      return result;
    }
    // Shape mismatch (or a scalar<->complex change): the current nested field names are lost.
    return allNestedFieldNames(curType);
  }

  /** Every struct field name anywhere within {@code type}, depth-first. */
  private static List<String> allNestedFieldNames(DeltaDataType type) {
    List<String> names = new ArrayList<>();
    if (type instanceof DeltaStructType struct && struct.getFields() != null) {
      for (DeltaStructField f : struct.getFields()) {
        names.add(f.getName());
        names.addAll(allNestedFieldNames(f.getType()));
      }
    } else if (type instanceof DeltaArrayType array) {
      names.addAll(allNestedFieldNames(array.getElementType()));
    } else if (type instanceof DeltaMapType map) {
      names.addAll(allNestedFieldNames(map.getKeyType()));
      names.addAll(allNestedFieldNames(map.getValueType()));
    }
    return names;
  }

  /**
   * Returns {@code true} if any top-level incoming column lacks a usable {@code
   * delta.columnMapping.physicalName} metadata entry -- the key is absent, or its value is not a
   * non-empty string. Used to detect a first-enable request where a hidden rename cannot be safely
   * ruled out, so a malformed physical name cannot slip past the guard and be consumed as an
   * identity downstream.
   */
  private static boolean hasAnyMissingPhysicalName(List<ColumnInfo> columns) {
    for (ColumnInfo c : columns) {
      DeltaStructField field = ColumnUtils.toStructField(c);
      DeltaStructFieldMetadata meta = field.getMetadata();
      Object phys = meta == null ? null : meta.get("delta.columnMapping.physicalName");
      if (!(phys instanceof String s) || s.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Detects column renames on the first-enable path: incoming physicalName matches a current
   * column's logical name (by Delta convention physicalName == current logical name on
   * first-enable) but the incoming logical name differs, indicating a hidden rename. Recurses into
   * nested struct/array/map types.
   *
   * @return human-readable rename descriptions, e.g. {@code "oldName -> newName"}
   */
  private static List<String> detectFirstEnableRenames(
      List<ColumnInfo> currentColumns, List<ColumnInfo> incomingColumns) {
    List<DeltaStructField> curFields =
        currentColumns.stream().map(ColumnUtils::toStructField).collect(Collectors.toList());
    List<DeltaStructField> incFields =
        incomingColumns.stream().map(ColumnUtils::toStructField).collect(Collectors.toList());
    return detectFirstEnableRenamesInFields(curFields, incFields);
  }

  /**
   * Recursive implementation that works with {@link DeltaStructField} directly. For each incoming
   * field whose physicalName matches a current field's logical name, checks whether the incoming
   * logical name differs (a first-enable rename), then recurses into matching type trees.
   */
  private static List<String> detectFirstEnableRenamesInFields(
      List<DeltaStructField> curFields, List<DeltaStructField> incFields) {
    Map<String, DeltaStructField> curByLower =
        curFields.stream()
            .collect(
                Collectors.toMap(f -> f.getName().toLowerCase(Locale.ROOT), Function.identity()));
    List<String> renames = new ArrayList<>();
    for (DeltaStructField inc : incFields) {
      DeltaStructFieldMetadata meta = inc.getMetadata();
      if (meta == null) continue;
      Object physObj = meta.get("delta.columnMapping.physicalName");
      if (!(physObj instanceof String physName)) continue;
      DeltaStructField cur = curByLower.get(physName.toLowerCase(Locale.ROOT));
      if (cur == null) continue;
      if (!inc.getName().equalsIgnoreCase(cur.getName())) {
        renames.add(cur.getName() + " -> " + inc.getName());
      }
      // Recurse into matching columns' type trees.
      renames.addAll(detectFirstEnableRenamesInTypes(cur.getType(), inc.getType()));
    }
    return renames;
  }

  /** Recurses into nested struct/array/map types for first-enable rename detection. */
  private static List<String> detectFirstEnableRenamesInTypes(
      DeltaDataType curType, DeltaDataType incType) {
    if (curType instanceof DeltaStructType curStruct
        && incType instanceof DeltaStructType incStruct) {
      return detectFirstEnableRenamesInFields(
          curStruct.getFields() != null ? curStruct.getFields() : List.of(),
          incStruct.getFields() != null ? incStruct.getFields() : List.of());
    } else if (curType instanceof DeltaArrayType curArr
        && incType instanceof DeltaArrayType incArr) {
      return detectFirstEnableRenamesInTypes(curArr.getElementType(), incArr.getElementType());
    } else if (curType instanceof DeltaMapType curMap && incType instanceof DeltaMapType incMap) {
      List<String> result =
          new ArrayList<>(
              detectFirstEnableRenamesInTypes(curMap.getKeyType(), incMap.getKeyType()));
      result.addAll(detectFirstEnableRenamesInTypes(curMap.getValueType(), incMap.getValueType()));
      return result;
    }
    return List.of();
  }

  /**
   * Represents a detected top-level column rename: the column's CM identity key stayed the same
   * (physicalName in name-mode, {@code delta.columnMapping.id} in id-mode) while its logical name
   * changed.
   */
  private record TopLevelRename(String oldLogicalName, String newLogicalName) {}

  /**
   * Detects top-level column renames by matching current and incoming columns on their CM identity
   * key (per-column: prefers {@code delta.columnMapping.id}; falls back to {@code
   * delta.columnMapping.physicalName}). Only top-level columns are compared; nested struct-field
   * renames ride inside the column's {@code type_json} and are not surfaced here.
   *
   * <p>Case-insensitive name comparison: a change from "A" to "a" is not considered a rename
   * (matching the standard approach of treating case-only changes as non-renames). This filtering
   * means the defensive same-name-rename check cannot be triggered in the current implementation.
   */
  private static List<TopLevelRename> detectTopLevelRenames(
      List<ColumnInfo> currentColumns, List<ColumnInfo> incomingColumns) {
    Map<String, String> currentByKey = new HashMap<>();
    for (ColumnInfo c : currentColumns) {
      String key = mappingKeyOf(c);
      if (key != null) currentByKey.put(key, c.getName());
    }
    Map<String, String> incomingByKey = new HashMap<>();
    for (ColumnInfo c : incomingColumns) {
      String key = mappingKeyOf(c);
      if (key != null) incomingByKey.put(key, c.getName());
    }
    List<TopLevelRename> renames = new ArrayList<>();
    for (Map.Entry<String, String> entry : currentByKey.entrySet()) {
      String oldName = entry.getValue();
      String newName = incomingByKey.get(entry.getKey());
      // equalsIgnoreCase: case-only changes ("A"→"a") are not renames.
      if (newName != null && !newName.equalsIgnoreCase(oldName)) {
        renames.add(new TopLevelRename(oldName, newName));
      }
    }
    return renames;
  }

  /**
   * Detects duplicate {@code delta.columnMapping.id} values in the incoming schema, recursing into
   * nested struct, array, and map types. Column IDs must be globally unique across all nesting
   * levels in id-mode.
   *
   * @return the first duplicate id found, or empty if all ids are unique
   */
  private static Optional<String> detectDuplicateCmId(List<ColumnInfo> incomingColumns) {
    Set<String> seen = new HashSet<>();
    for (ColumnInfo c : incomingColumns) {
      DeltaStructField field = ColumnUtils.toStructField(c);
      Optional<String> dup = collectCmIds(field, seen);
      if (dup.isPresent()) return dup;
    }
    return Optional.empty();
  }

  /**
   * Detects a {@code delta.columnMapping.physicalName} value shared by two top-level columns in the
   * incoming schema. Only top-level columns are compared, since physical names are unique within a
   * sibling group but may legitimately repeat across nesting levels.
   *
   * @return the first duplicate top-level physical name found, or empty if all are unique
   */
  private static Optional<String> detectDuplicateTopLevelPhysicalName(
      List<ColumnInfo> incomingColumns) {
    Set<String> seen = new HashSet<>();
    for (ColumnInfo c : incomingColumns) {
      DeltaStructFieldMetadata meta = ColumnUtils.toStructField(c).getMetadata();
      Object phys = meta == null ? null : meta.get("delta.columnMapping.physicalName");
      if (phys instanceof String s && !s.isEmpty() && !seen.add(s)) {
        return Optional.of(s);
      }
    }
    return Optional.empty();
  }

  /** Checks {@code field}'s CM id and recurses into its type for further nested ids. */
  private static Optional<String> collectCmIds(DeltaStructField field, Set<String> seen) {
    DeltaStructFieldMetadata meta = field.getMetadata();
    if (meta != null) {
      Object idObj = meta.get("delta.columnMapping.id");
      if (idObj != null) {
        String id = idObj.toString();
        if (!seen.add(id)) {
          return Optional.of(id);
        }
      }
    }
    return collectCmIdsInType(field.getType(), seen);
  }

  /** Recurses into complex types (struct / array / map) to collect CM ids. */
  private static Optional<String> collectCmIdsInType(DeltaDataType type, Set<String> seen) {
    if (type instanceof DeltaStructType struct) {
      List<DeltaStructField> fields = struct.getFields();
      if (fields != null) {
        for (DeltaStructField f : fields) {
          Optional<String> dup = collectCmIds(f, seen);
          if (dup.isPresent()) return dup;
        }
      }
    } else if (type instanceof DeltaArrayType array) {
      return collectCmIdsInType(array.getElementType(), seen);
    } else if (type instanceof DeltaMapType map) {
      Optional<String> dup = collectCmIdsInType(map.getKeyType(), seen);
      if (dup.isPresent()) return dup;
      return collectCmIdsInType(map.getValueType(), seen);
    }
    return Optional.empty();
  }

  /**
   * Detects column-mapping key value reassignment on retained columns (columns with the same
   * logical name in both the current and incoming schemas). In id-mode, the {@code
   * delta.columnMapping.id} value must not change; in name-mode, the {@code
   * delta.columnMapping.physicalName} value must not change. Recurses into nested struct fields.
   * Top-level logical names vacated by a rename (in {@code renamedAwayLowerNames}) are excluded, so
   * an added column that reuses a renamed-away name is not misread as a reassignment.
   *
   * @return sorted list of logical names of columns whose CM key value was reassigned
   */
  private static List<String> detectCmValueReassignment(
      List<ColumnInfo> currentColumns,
      List<ColumnInfo> incomingColumns,
      String mode,
      Set<String> renamedAwayLowerNames) {
    Map<String, ColumnInfo> currentByLower =
        currentColumns.stream()
            .filter(c -> !renamedAwayLowerNames.contains(c.getName().toLowerCase(Locale.ROOT)))
            .collect(
                Collectors.toMap(c -> c.getName().toLowerCase(Locale.ROOT), Function.identity()));
    List<String> violations = new ArrayList<>();
    for (ColumnInfo inc : incomingColumns) {
      ColumnInfo cur = currentByLower.get(inc.getName().toLowerCase(Locale.ROOT));
      if (cur != null) {
        DeltaStructField curField = ColumnUtils.toStructField(cur);
        DeltaStructField incField = ColumnUtils.toStructField(inc);
        violations.addAll(checkCmValueConsistency(curField, incField, mode, inc.getName()));
      }
    }
    return violations;
  }

  /**
   * Checks that the CM key value of {@code incField} matches {@code curField} for the given {@code
   * mode}, and recurses into any nested struct / array / map types. Returns the logical names of
   * fields (at the path described by {@code fieldLabel}) whose CM key value changed.
   */
  private static List<String> checkCmValueConsistency(
      DeltaStructField curField, DeltaStructField incField, String mode, String fieldLabel) {
    List<String> violations = new ArrayList<>();
    String cmKey =
        "id".equals(mode) ? "delta.columnMapping.id" : "delta.columnMapping.physicalName";
    String curValue = getCmKeyValue(curField, cmKey);
    String incValue = getCmKeyValue(incField, cmKey);
    // Only flag when BOTH schemas carry the key (presence-only strip is handled separately).
    if (curValue != null && incValue != null && !curValue.equals(incValue)) {
      violations.add(fieldLabel);
    }
    // Recurse into nested struct types via the type trees.
    violations.addAll(
        checkCmValueConsistencyInTypes(curField.getType(), incField.getType(), mode, fieldLabel));
    return violations;
  }

  /** Recurses into matching complex-type pairs to check CM value consistency of nested fields. */
  private static List<String> checkCmValueConsistencyInTypes(
      DeltaDataType curType, DeltaDataType incType, String mode, String parentLabel) {
    List<String> violations = new ArrayList<>();
    if (curType instanceof DeltaStructType curStruct
        && incType instanceof DeltaStructType incStruct) {
      List<DeltaStructField> curFields =
          curStruct.getFields() != null ? curStruct.getFields() : List.of();
      // Build a map of current nested fields by lower-case logical name.
      Map<String, DeltaStructField> curByLower =
          curFields.stream()
              .collect(
                  Collectors.toMap(f -> f.getName().toLowerCase(Locale.ROOT), Function.identity()));
      List<DeltaStructField> incFields =
          incStruct.getFields() != null ? incStruct.getFields() : List.of();
      for (DeltaStructField incF : incFields) {
        DeltaStructField curF = curByLower.get(incF.getName().toLowerCase(Locale.ROOT));
        if (curF != null) {
          violations.addAll(
              checkCmValueConsistency(curF, incF, mode, parentLabel + "." + incF.getName()));
        }
      }
    } else if (curType instanceof DeltaArrayType curArr
        && incType instanceof DeltaArrayType incArr) {
      violations.addAll(
          checkCmValueConsistencyInTypes(
              curArr.getElementType(), incArr.getElementType(), mode, parentLabel));
    } else if (curType instanceof DeltaMapType curMap && incType instanceof DeltaMapType incMap) {
      violations.addAll(
          checkCmValueConsistencyInTypes(
              curMap.getKeyType(), incMap.getKeyType(), mode, parentLabel));
      violations.addAll(
          checkCmValueConsistencyInTypes(
              curMap.getValueType(), incMap.getValueType(), mode, parentLabel));
    }
    return violations;
  }

  /**
   * Returns the string value of {@code cmKey} from {@code field}'s metadata, or {@code null} if the
   * key is absent or its value is null.
   */
  private static String getCmKeyValue(DeltaStructField field, String cmKey) {
    DeltaStructFieldMetadata meta = field.getMetadata();
    if (meta == null) return null;
    Object val = meta.get(cmKey);
    return val != null ? val.toString() : null;
  }

  /**
   * Returns the column-mapping identity key for a DAO column. Prefers {@code
   * delta.columnMapping.id} (id-mode), falls back to {@code delta.columnMapping.physicalName}
   * (name-mode), and returns {@code null} if neither is present (no column mapping). The returned
   * key includes a namespace prefix ({@code "id:"} or {@code "phys:"}) to prevent a numeric
   * physical-name value from colliding with a real CM id.
   */
  private static String mappingKeyOf(ColumnInfoDAO dao) {
    return mappingKeyOf(ColumnUtils.toStructField(dao.toColumnInfo()));
  }

  /**
   * Returns the column-mapping identity key for a {@link ColumnInfo}. See {@link
   * #mappingKeyOf(ColumnInfoDAO)}.
   */
  private static String mappingKeyOf(ColumnInfo column) {
    return mappingKeyOf(ColumnUtils.toStructField(column));
  }

  /** Shared CM identity key extractor for a {@link DeltaStructField}. */
  private static String mappingKeyOf(DeltaStructField field) {
    DeltaStructFieldMetadata meta = field.getMetadata();
    if (meta == null) {
      return null;
    }
    Object id = meta.get("delta.columnMapping.id");
    if (id != null) {
      return "id:" + id;
    }
    Object physName = meta.get("delta.columnMapping.physicalName");
    if (physName instanceof String s) {
      return "phys:" + s;
    }
    return null;
  }

  /**
   * Rejects a {@code set-columns} where a column present in both the existing schema and the
   * incoming schema (matched by logical name) has lost the ACTIVE MODE'S column-mapping identity
   * key. Mode-specific (checks only the active mode's key -- {@code delta.columnMapping.id} for
   * id-mode, {@code delta.columnMapping.physicalName} for name-mode) and recursive into nested
   * struct/array/map types.
   *
   * <p>Columns whose existing schema carries no active-mode CM key are not subject to this check.
   *
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} naming the offending columns
   */
  private static void validateNoCmIdentityStripped(
      List<ColumnInfoDAO> existingDaos,
      List<ColumnInfo> incomingColumns,
      String columnMappingMode) {
    String activeCmKey = activeCmKeyForMode(columnMappingMode);
    if (activeCmKey == null) return; // no active mode, nothing to check
    List<DeltaStructField> curFields =
        existingDaos.stream()
            .map(d -> ColumnUtils.toStructField(d.toColumnInfo()))
            .collect(Collectors.toList());
    List<DeltaStructField> incFields =
        incomingColumns.stream().map(ColumnUtils::toStructField).collect(Collectors.toList());
    List<String> stripped = detectStrippedCmKeys(curFields, incFields, activeCmKey);
    if (!stripped.isEmpty()) {
      stripped.sort(String::compareTo);
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "set-columns dropped column mapping metadata for column(s): "
              + String.join(", ", stripped));
    }
  }

  /**
   * Returns the logical names of fields (at the current nesting level) that are retained in {@code
   * incFields} (by case-insensitive logical name) but have lost the {@code activeCmKey} from their
   * metadata. Recurses into nested struct/array/map types.
   */
  private static List<String> detectStrippedCmKeys(
      List<DeltaStructField> curFields, List<DeltaStructField> incFields, String activeCmKey) {
    Map<String, DeltaStructField> incByLower =
        incFields.stream()
            .collect(
                Collectors.toMap(f -> f.getName().toLowerCase(Locale.ROOT), Function.identity()));
    List<String> stripped = new ArrayList<>();
    for (DeltaStructField cur : curFields) {
      // Only flag fields that currently have the active CM key.
      if (!hasCmKeyInFieldTopLevel(cur, activeCmKey)) continue;
      DeltaStructField inc = incByLower.get(cur.getName().toLowerCase(Locale.ROOT));
      if (inc == null) continue; // dropped column, not a strip of an existing one
      if (!hasCmKeyInFieldTopLevel(inc, activeCmKey)) {
        stripped.add(cur.getName());
      }
      // Recurse into nested type trees for the retained field.
      stripped.addAll(detectStrippedCmKeysInTypes(cur.getType(), inc.getType(), activeCmKey));
    }
    return stripped;
  }

  /** Recurses into nested struct/array/map types for the CM-key-strip check. */
  private static List<String> detectStrippedCmKeysInTypes(
      DeltaDataType curType, DeltaDataType incType, String activeCmKey) {
    if (curType instanceof DeltaStructType curStruct
        && incType instanceof DeltaStructType incStruct) {
      return detectStrippedCmKeys(
          curStruct.getFields() != null ? curStruct.getFields() : List.of(),
          incStruct.getFields() != null ? incStruct.getFields() : List.of(),
          activeCmKey);
    } else if (curType instanceof DeltaArrayType curArr
        && incType instanceof DeltaArrayType incArr) {
      return detectStrippedCmKeysInTypes(
          curArr.getElementType(), incArr.getElementType(), activeCmKey);
    } else if (curType instanceof DeltaMapType curMap && incType instanceof DeltaMapType incMap) {
      List<String> result =
          new ArrayList<>(
              detectStrippedCmKeysInTypes(curMap.getKeyType(), incMap.getKeyType(), activeCmKey));
      result.addAll(
          detectStrippedCmKeysInTypes(curMap.getValueType(), incMap.getValueType(), activeCmKey));
      return result;
    }
    return List.of();
  }

  /** Returns {@code true} if this specific field's OWN metadata contains {@code cmKey}. */
  private static boolean hasCmKeyInFieldTopLevel(DeltaStructField field, String cmKey) {
    DeltaStructFieldMetadata meta = field.getMetadata();
    return meta != null && meta.get(cmKey) != null;
  }

  /**
   * Returns {@code true} if {@code field}'s metadata contains {@code cmKey} OR any nested field in
   * its type tree does. Used for the schema-wide check that detects whether the active CM key is
   * present anywhere in the schema.
   */
  private static boolean hasCmKeyInField(DeltaStructField field, String cmKey) {
    if (hasCmKeyInFieldTopLevel(field, cmKey)) return true;
    return hasCmKeyInType(field.getType(), cmKey);
  }

  /** Recurses into type trees to find any field carrying {@code cmKey}. */
  private static boolean hasCmKeyInType(DeltaDataType type, String cmKey) {
    if (type instanceof DeltaStructType struct) {
      List<DeltaStructField> fields = struct.getFields();
      if (fields != null) {
        for (DeltaStructField f : fields) {
          if (hasCmKeyInField(f, cmKey)) return true;
        }
      }
    } else if (type instanceof DeltaArrayType array) {
      return hasCmKeyInType(array.getElementType(), cmKey);
    } else if (type instanceof DeltaMapType map) {
      return hasCmKeyInType(map.getKeyType(), cmKey) || hasCmKeyInType(map.getValueType(), cmKey);
    }
    return false;
  }

  /**
   * Returns {@code true} if any column in {@code columns} (at any nesting level) contains the given
   * {@code cmKey} in its metadata. Used for the mode-specific check that rejects dropping the
   * active mode's column-mapping key.
   */
  private static boolean hasCmKeyAnywhere(List<ColumnInfo> columns, String cmKey) {
    for (ColumnInfo col : columns) {
      if (hasCmKeyInField(ColumnUtils.toStructField(col), cmKey)) return true;
    }
    return false;
  }

  /**
   * Returns the metadata key string for the active column-mapping mode: {@code "id"} → {@code
   * "delta.columnMapping.id"}, {@code "name"} → {@code "delta.columnMapping.physicalName"},
   * anything else → {@code null} (no active CM key).
   */
  private static String activeCmKeyForMode(String mode) {
    return switch (mode) {
      case "id" -> "delta.columnMapping.id";
      case "name" -> "delta.columnMapping.physicalName";
      default -> null;
    };
  }

  /**
   * Normalises and validates the raw {@code delta.columnMapping.mode} property value. Returns
   * {@code "none"} for absent/null values; throws {@link ErrorCode#INVALID_ARGUMENT} for any value
   * that is not {@code "none"}, {@code "name"}, or {@code "id"}.
   */
  private static String normalizeColumnMappingMode(String rawMode) {
    if (rawMode == null) return "none";
    String mode = rawMode.trim().toLowerCase(Locale.ROOT);
    return switch (mode) {
      case "none", "name", "id" -> mode;
      default ->
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT, "Invalid column mapping mode: " + rawMode);
    };
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
      MutablePropertyMap properties, DeltaDomainMetadataUpdates updates) {
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

  private static void applySetTableComment(TableInfoDAO dao, DeltaSetTableCommentUpdate update) {
    ValidationUtils.checkNotNull(update.getComment(), "set-table-comment requires a comment.");
    dao.setComment(update.getComment());
  }

  private static void applyUpdateSnapshotVersion(
      TableInfoDAO dao, MutablePropertyMap properties, DeltaUpdateSnapshotVersionUpdate update) {
    if (!TableType.EXTERNAL.toString().equals(dao.getType())) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "update-metadata-snapshot-version is only supported for EXTERNAL Delta tables; "
              + "for MANAGED tables, use the Delta commit endpoint.");
    }
    ValidationUtils.checkNotNull(
        update.getLastCommitVersion(),
        "update-metadata-snapshot-version requires last-commit-version.");
    ValidationUtils.checkNotNull(
        update.getLastCommitTimestampMs(),
        "update-metadata-snapshot-version requires last-commit-timestamp-ms.");
    properties.put(
        TableProperties.LAST_UPDATE_VERSION, String.valueOf(update.getLastCommitVersion()));
    properties.put(
        TableProperties.LAST_COMMIT_TIMESTAMP, String.valueOf(update.getLastCommitTimestampMs()));
  }

  // ---------------------------------------------------------------------- commit + backfill

  /**
   * Mapper-side preparation for the {@code add-commit} and {@code set-latest-backfilled-version}
   * actions: cross-action validation plus snapshot-property bookkeeping. Returns the {@link
   * CommitDispatch} the caller forwards to {@code
   * DeltaCommitRepository.applyCommitAndBackfillInSession} so the mapper stays free of the
   * commit-repo dependency.
   *
   * <ul>
   *   <li>require MANAGED -- both actions are only legal on UC catalog-managed Delta tables (DELTA
   *       format is guaranteed by the caller's prior {@code requireDeltaTable});
   *   <li>extract and shape-validate uniform fields when {@code add-commit} carries them;
   *   <li>pin uniform.iceberg.converted-delta-version equal to commit.version (the commit-time
   *       check that the create-time uniform validator can't run because there is no commit version
   *       at create);
   *   <li>run the UniForm presence-consistency check against the post-update property map;
   *   <li>stamp {@code delta.lastUpdateVersion} / {@code delta.lastCommitTimestamp} when the commit
   *       is metadata-changing;
   *   <li>read {@code latest-published-version} off the backfill action and reject if it's null.
   * </ul>
   */
  private static CommitDispatch prepareCommitAndBackfill(
      TableInfoDAO dao,
      MutablePropertyMap properties,
      Optional<DeltaAddCommitUpdate> addCommitOpt,
      Optional<DeltaSetLatestBackfilledVersionUpdate> backfillOpt,
      boolean hasManagedTableMetadataChange) {
    requireManaged(dao);
    Optional<DeltaUniformUtils.UniformIcebergFields> uniformFields = Optional.empty();
    Optional<DeltaCommit> commitOpt = Optional.empty();
    if (addCommitOpt.isPresent()) {
      DeltaAddCommitUpdate addCommit = addCommitOpt.get();
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
    return new CommitDispatch(
        commitOpt.map(DeltaUpdateTableMapper::toUcCommitInfo),
        uniformFields,
        latestBackfilledVersion);
  }

  /**
   * Convert a {@link DeltaCommit} into the UC {@link DeltaCommitInfo} shape so the Delta update
   * path can flow through the shared commit-log helpers, which speak the UC wire shape.
   */
  private static DeltaCommitInfo toUcCommitInfo(DeltaCommit commit) {
    return new DeltaCommitInfo()
        .version(commit.getVersion())
        .timestamp(commit.getTimestamp())
        .fileName(commit.getFileName())
        .fileSize(commit.getFileSize())
        .fileModificationTimestamp(commit.getFileModificationTimestamp());
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
