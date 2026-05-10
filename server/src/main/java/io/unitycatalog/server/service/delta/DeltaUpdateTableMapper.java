package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.delta.model.AssertEtag;
import io.unitycatalog.server.delta.model.AssertTableUUID;
import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.DomainMetadataUpdates;
import io.unitycatalog.server.delta.model.RemoveDomainMetadataUpdate;
import io.unitycatalog.server.delta.model.RemovePropertiesUpdate;
import io.unitycatalog.server.delta.model.SetDomainMetadataUpdate;
import io.unitycatalog.server.delta.model.SetPropertiesUpdate;
import io.unitycatalog.server.delta.model.SetProtocolUpdate;
import io.unitycatalog.server.delta.model.SetTableCommentUpdate;
import io.unitycatalog.server.delta.model.TableRequirement;
import io.unitycatalog.server.delta.model.TableUpdate;
import io.unitycatalog.server.delta.model.UpdateSnapshotVersionUpdate;
import io.unitycatalog.server.delta.model.UpdateTableRequest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.MutablePropertyMap;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.service.delta.DeltaConsts.DomainMetadataNames;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

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

  /** Domain metadata name to which derived property key it owns. */
  private static final Map<String, String> DOMAIN_TO_PROPERTY_KEY =
      Map.of(
          DomainMetadataNames.CLUSTERING, TableProperties.CLUSTERING_COLUMNS,
          DomainMetadataNames.ROW_TRACKING, TableProperties.ROW_TRACKING_ROW_ID_HIGH_WATER_MARK);

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
        assertTableUuid = fillOnce(assertTableUuid, u);
      } else if (req instanceof AssertEtag e) {
        assertEtag = fillOnce(assertEtag, e);
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
    private Optional<SetTableCommentUpdate> setTableComment = Optional.empty();
    private Optional<SetDomainMetadataUpdate> setDomainMetadata = Optional.empty();
    private Optional<RemoveDomainMetadataUpdate> removeDomainMetadata = Optional.empty();
    private Optional<UpdateSnapshotVersionUpdate> updateSnapshotVersion = Optional.empty();

    void putOnce(TableUpdate update) {
      if (update instanceof SetPropertiesUpdate u) {
        setProperties = fillOnce(setProperties, u);
      } else if (update instanceof RemovePropertiesUpdate u) {
        removeProperties = fillOnce(removeProperties, u);
      } else if (update instanceof SetProtocolUpdate u) {
        setProtocol = fillOnce(setProtocol, u);
      } else if (update instanceof SetTableCommentUpdate u) {
        setTableComment = fillOnce(setTableComment, u);
      } else if (update instanceof SetDomainMetadataUpdate u) {
        setDomainMetadata = fillOnce(setDomainMetadata, u);
      } else if (update instanceof RemoveDomainMetadataUpdate u) {
        removeDomainMetadata = fillOnce(removeDomainMetadata, u);
      } else if (update instanceof UpdateSnapshotVersionUpdate u) {
        updateSnapshotVersion = fillOnce(updateSnapshotVersion, u);
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

  private static <T extends TableUpdate> Optional<T> fillOnce(Optional<T> slot, T value) {
    if (slot.isPresent()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "At most one " + value.getAction() + " is allowed per request.");
    }
    return Optional.of(value);
  }

  private static <T extends TableRequirement> Optional<T> fillOnce(Optional<T> slot, T value) {
    if (slot.isPresent()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "At most one " + value.getType() + " is allowed per request.");
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

  /** Shared by the {@code loadTable} response builder and the {@code assert-etag} check. */
  public static String computeEtag(TableInfoDAO dao) {
    return dao.getUpdatedAt() != null
        ? "etag-" + dao.getUpdatedAt().getTime()
        : "etag-" + dao.getId();
  }

  // ---------------------------------------------------------------------- apply updates

  /** Actions run in canonical order, not request order. */
  public static void applyUpdates(
      TableInfoDAO dao, MutablePropertyMap properties, CollectedRequest collected) {
    CollectedUpdates c = collected.updates();
    c.setProtocol.ifPresent(u -> applySetProtocol(properties, u.getProtocol()));
    c.setProperties.ifPresent(u -> applySetProperties(properties, u.getUpdates()));
    c.removeProperties.ifPresent(u -> applyRemoveProperties(properties, u.getRemovals()));
    c.setDomainMetadata.ifPresent(u -> applySetDomainMetadata(properties, u.getUpdates()));
    c.removeDomainMetadata.ifPresent(u -> applyRemoveDomainMetadata(properties, u.getDomains()));
    c.setTableComment.ifPresent(u -> dao.setComment(u.getComment()));
    c.updateSnapshotVersion.ifPresent(u -> applyUpdateSnapshotVersion(dao, properties, u));
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

  /** Full replacement of the protocol block only; other stored property keys are left alone. */
  private static void applySetProtocol(MutablePropertyMap properties, DeltaProtocol protocol) {
    if (protocol == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "set-protocol requires a protocol.");
    }
    properties.removeMatchingPrefix(TableProperties.FEATURE_PREFIX);
    Map<String, String> derived = new HashMap<>();
    DeltaPropertyMapper.deriveFromProtocol(derived, protocol);
    properties.putAll(derived);
  }

  private static void applySetDomainMetadata(
      MutablePropertyMap properties, DomainMetadataUpdates updates) {
    if (updates == null) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "set-domain-metadata requires an updates block.");
    }
    Map<String, String> derived = new HashMap<>();
    DeltaPropertyMapper.deriveFromDomainMetadata(derived, updates);
    if (!derived.isEmpty()) {
      properties.putAll(derived);
    }
  }

  private static void applyRemoveDomainMetadata(
      MutablePropertyMap properties, List<String> domains) {
    if (domains == null || domains.isEmpty()) {
      return;
    }
    for (String domain : domains) {
      String propertyKey = DOMAIN_TO_PROPERTY_KEY.get(domain);
      if (propertyKey == null) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT, "Unknown domain in remove-domain-metadata: " + domain);
      }
      properties.remove(propertyKey);
    }
  }

  private static void applyUpdateSnapshotVersion(
      TableInfoDAO dao, MutablePropertyMap properties, UpdateSnapshotVersionUpdate update) {
    if (!TableType.EXTERNAL.toString().equals(dao.getType())) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "update-metadata-snapshot-version is only supported for EXTERNAL tables.");
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
}
