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
   * catalog semantics where column identity is refreshed on every set-columns). A retained column
   * that drops its active-mode column-mapping key is rejected via {@link
   * #validateNoCmIdentityStripped} before the swap runs.
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
    // Reject any retained column (same logical name) that lost its active mode column-mapping key.
    // Mode-specific and recursive into nested types.
    if (setSchema.isPresent()) {
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
