package io.unitycatalog.server.service;

import static io.unitycatalog.server.auth.AuthorizeExpressions.SCIM_PROVISIONING;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.utils.Scim2Utils.asGroupResource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Patch;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.Produces;
import com.linecorp.armeria.server.annotation.Put;
import com.linecorp.armeria.server.annotation.StatusCode;
import com.unboundid.scim2.common.Path;
import com.unboundid.scim2.common.exceptions.BadRequestException;
import com.unboundid.scim2.common.exceptions.ForbiddenException;
import com.unboundid.scim2.common.exceptions.NotImplementedException;
import com.unboundid.scim2.common.exceptions.ResourceConflictException;
import com.unboundid.scim2.common.exceptions.ResourceNotFoundException;
import com.unboundid.scim2.common.exceptions.ScimException;
import com.unboundid.scim2.common.exceptions.ServerErrorException;
import com.unboundid.scim2.common.filters.EqualFilter;
import com.unboundid.scim2.common.filters.Filter;
import com.unboundid.scim2.common.messages.ListResponse;
import com.unboundid.scim2.common.messages.PatchOpType;
import com.unboundid.scim2.common.messages.PatchOperation;
import com.unboundid.scim2.common.messages.PatchRequest;
import com.unboundid.scim2.common.types.GroupResource;
import com.unboundid.scim2.common.types.Member;
import com.unboundid.scim2.common.types.Meta;
import com.unboundid.scim2.common.utils.FilterEvaluator;
import com.unboundid.scim2.common.utils.Parser;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.Scim2RuntimeException;
import io.unitycatalog.server.persist.GroupRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.model.Group;
import io.unitycatalog.server.utils.IdentityUtils;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * SCIM2 group management: {@code /scim2/Groups}.
 *
 * <p>A group is flat: its members are users, never other groups. The SCIM {@code displayName} is
 * the group's name and is unique across the server; {@code externalId} is the IdP's id for it.
 * Every request that changes a group is applied whole or not at all.
 *
 * <p>PATCH supports {@code add}, {@code remove} and {@code replace} on {@code members}, and {@code
 * replace} of {@code displayName}, by path or, for {@code replace}, as a value without a path.
 * Anything else is answered 501.
 *
 * <p>A group is owned by the admin, or by the IdP whose SCIM token created it. An IdP sees and
 * changes only its own groups; any other is not found for it. A group has two member lists, the
 * IdP's and the admin's, and each caller changes only its own: an IdP's PUT replaces only its list.
 * On an IdP's group the admin can only change its member list.
 */
public class Scim2GroupService implements ScimService {
  private static final String MEMBERS = "members";
  private static final String DISPLAY_NAME = "displayName";
  private static final String EXTERNAL_ID = "externalId";
  // The width of the displayName and externalId columns.
  private static final int MAX_LENGTH = 255;

  private final UnityCatalogAuthorizer authorizer;
  private final GroupRepository groupRepository;

  public Scim2GroupService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    this.authorizer = authorizer;
    this.groupRepository = repositories.getGroupRepository();
  }

  /**
   * Lists the groups that match {@code filter}, ordered by name. {@code totalResults} counts every
   * match; {@code startIndex} (from 1) and {@code count} pick the page.
   */
  @Get("")
  @Produces("application/scim+json")
  @StatusCode(200)
  @AuthorizeExpression(SCIM_PROVISIONING)
  @AuthorizeResourceKey(METASTORE)
  public ListResponse<GroupResource> getScimGroups(
      @Param("filter") Optional<String> filter,
      @Param("startIndex") Optional<Integer> startIndex,
      @Param("count") Optional<Integer> count) {
    final Filter groupFilter =
        filter.filter(f -> !f.isEmpty()).<Filter>map(this::parseFilter).orElse(null);
    FilterEvaluator filterEvaluator = new FilterEvaluator();

    String caller = IdentityUtils.callerOwner();
    List<GroupResource> matches =
        scim(groupRepository::listGroups).stream()
            .filter(group -> isVisible(group, caller))
            .map(group -> asGroupResource(group, caller))
            .filter(group -> match(filterEvaluator, groupFilter, group))
            .toList();
    int start = Math.max(startIndex.orElse(1), 1);
    int from = Math.min(start - 1, matches.size());
    int to = (int) Math.min((long) from + Math.max(count.orElse(50), 0), matches.size());
    List<GroupResource> page = matches.subList(from, to);

    Meta meta = new Meta();
    meta.setCreated(Calendar.getInstance());
    meta.setLastModified(Calendar.getInstance());
    meta.setResourceType("Group");
    ListResponse<GroupResource> groupResources =
        new ListResponse<>(matches.size(), page, start, page.size());
    groupResources.setMeta(meta);
    return groupResources;
  }

  @Post("")
  @Produces("application/scim+json")
  @StatusCode(201)
  @AuthorizeExpression(SCIM_PROVISIONING)
  @AuthorizeResourceKey(METASTORE)
  public GroupResource createScimGroup(GroupResource groupResource) {
    String name = requireDisplayName(groupResource.getDisplayName());
    String externalId = checkLength(EXTERNAL_ID, groupResource.getExternalId());
    Set<UUID> memberIds = memberIds(groupResource.getMembers());
    String caller = IdentityUtils.callerOwner();
    return asGroupResource(
        scim(() -> groupRepository.createGroup(name, externalId, caller, memberIds)), caller);
  }

  @Get("/{id}")
  @Produces("application/scim+json")
  @StatusCode(200)
  @AuthorizeExpression(SCIM_PROVISIONING)
  @AuthorizeResourceKey(METASTORE)
  public GroupResource getGroup(@Param("id") String id) {
    String caller = IdentityUtils.callerOwner();
    return asGroupResource(visibleGroup(id, caller), caller);
  }

  /**
   * Replaces the group's name, external id and the caller's member list; what the body leaves out
   * is cleared. Only the group's owner can.
   */
  @Put("/{id}")
  @Produces("application/scim+json")
  @StatusCode(200)
  @AuthorizeExpression(SCIM_PROVISIONING)
  @AuthorizeResourceKey(METASTORE)
  public GroupResource updateGroup(@Param("id") String id, GroupResource groupResource) {
    String name = requireDisplayName(groupResource.getDisplayName());
    String externalId = checkLength(EXTERNAL_ID, groupResource.getExternalId());
    Set<UUID> memberIds = memberIds(groupResource.getMembers());
    String caller = IdentityUtils.callerOwner();
    requireOwner(visibleGroup(id, caller), caller);
    boolean adminAdded = isAdmin(caller);
    return update(
        id,
        caller,
        group ->
            group.toBuilder()
                .name(name)
                .externalId(externalId)
                .build()
                .withMemberIds(adminAdded, memberIds));
  }

  @Patch("/{id}")
  @Produces("application/scim+json")
  @StatusCode(200)
  @AuthorizeExpression(SCIM_PROVISIONING)
  @AuthorizeResourceKey(METASTORE)
  public GroupResource patchGroup(@Param("id") String id, PatchRequest patchRequest) {
    if (patchRequest.getOperations().isEmpty()) {
      throw new Scim2RuntimeException(new BadRequestException("PATCH has no operations."));
    }
    String caller = IdentityUtils.callerOwner();
    Group group = visibleGroup(id, caller);
    // On an IdP's group the admin changes only its member list.
    if (!patchRequest.getOperations().stream().allMatch(Scim2GroupService::isMembersOperation)) {
      requireOwner(group, caller);
    }
    boolean adminAdded = isAdmin(caller);
    // Every operation is checked before any is applied.
    List<UnaryOperator<Group>> changes =
        patchRequest.getOperations().stream().map(op -> toChange(op, adminAdded)).toList();
    return update(
        id,
        caller,
        current -> {
          Group changed = current;
          for (UnaryOperator<Group> change : changes) {
            changed = change.apply(changed);
          }
          return changed;
        });
  }

  /** Clears the group's grants, then deletes the group and its memberships. Only its owner can. */
  @Delete("/{id}")
  @AuthorizeExpression(SCIM_PROVISIONING)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse deleteGroup(@Param("id") String id) {
    String caller = IdentityUtils.callerOwner();
    Group group = visibleGroup(id, caller);
    requireOwner(group, caller);
    authorizer.clearAuthorizationsForPrincipal(group.getId());
    scim(
        () -> {
          groupRepository.deleteGroup(id);
          return null;
        });
    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  private GroupResource update(String id, String caller, UnaryOperator<Group> change) {
    return asGroupResource(scim(() -> groupRepository.updateGroup(id, change)), caller);
  }

  private static boolean isAdmin(String caller) {
    return IdentityUtils.ADMIN_OWNER.equals(caller);
  }

  /** The admin sees every group; an IdP sees only its own. */
  private static boolean isVisible(Group group, String caller) {
    return isAdmin(caller) || caller.equals(group.getOwner());
  }

  /** The group, if {@code caller} may see it; another IdP's group is not found for an IdP. */
  private Group visibleGroup(String id, String caller) {
    Group group = scim(() -> groupRepository.getGroup(id));
    if (!isVisible(group, caller)) {
      throw new Scim2RuntimeException(new ResourceNotFoundException("Group not found: " + id));
    }
    return group;
  }

  /** Refuses the admin a change to an IdP's group beyond its own member list. */
  private static void requireOwner(Group group, String caller) {
    if (!caller.equals(group.getOwner())) {
      throw new Scim2RuntimeException(
          new ForbiddenException(
              "Group " + group.getName() + " is managed by identity provider " + group.getOwner()));
    }
  }

  private static boolean isMembersOperation(PatchOperation operation) {
    Path path = operation.getPath();
    return path != null
        && path.size() == 1
        && MEMBERS.equalsIgnoreCase(path.getElement(0).getAttribute());
  }

  /** The change {@code operation} makes; a member change acts on the caller's list. */
  private UnaryOperator<Group> toChange(PatchOperation operation, boolean adminAdded) {
    Path path = operation.getPath();
    boolean replace = operation.getOpType() == PatchOpType.REPLACE;
    try {
      if (path == null && replace) {
        // Okta renames with a value and no path: {"id": ..., "displayName": ...}.
        return renameFromValue(operation.getJsonNode());
      }
      if (path != null && path.size() == 1) {
        Path.Element element = path.getElement(0);
        if (replace
            && DISPLAY_NAME.equalsIgnoreCase(element.getAttribute())
            && element.getValueFilter() == null) {
          return renameTo(requireDisplayName(operation.getValue(String.class)));
        }
        if (MEMBERS.equalsIgnoreCase(element.getAttribute())) {
          Optional<UnaryOperator<Group>> change =
              membersChange(operation, element.getValueFilter(), adminAdded);
          if (change.isPresent()) {
            return change.get();
          }
        }
      }
    } catch (JsonProcessingException | IllegalArgumentException e) {
      throw new Scim2RuntimeException(new BadRequestException("Invalid PATCH value: " + operation));
    } catch (ScimException e) {
      throw new Scim2RuntimeException(e);
    }
    throw new Scim2RuntimeException(
        new NotImplementedException("PATCH is not supported for: " + operation));
  }

  private UnaryOperator<Group> renameFromValue(JsonNode value) throws BadRequestException {
    Set<String> unsupported = new LinkedHashSet<>();
    String displayName = null;
    for (Iterator<Map.Entry<String, JsonNode>> fields = value.fields(); fields.hasNext(); ) {
      Map.Entry<String, JsonNode> field = fields.next();
      if (DISPLAY_NAME.equalsIgnoreCase(field.getKey())) {
        displayName = field.getValue().asText(null);
      } else if (!"id".equalsIgnoreCase(field.getKey())) {
        unsupported.add(field.getKey());
      }
    }
    if (!unsupported.isEmpty()) {
      throw new Scim2RuntimeException(
          new NotImplementedException("PATCH is not supported for: " + unsupported));
    }
    return renameTo(requireDisplayName(displayName));
  }

  private static UnaryOperator<Group> renameTo(String name) {
    return group -> group.toBuilder().name(name).build();
  }

  /**
   * The change a {@code members} operation makes, or empty if the operation is not supported: a
   * filter is only supported on {@code remove}, in the form {@code members[value eq "<id>"]}, and
   * {@code remove} needs it.
   */
  private Optional<UnaryOperator<Group>> membersChange(
      PatchOperation operation, Filter filter, boolean adminAdded)
      throws ScimException, JsonProcessingException {
    if (filter != null) {
      if (operation.getOpType() != PatchOpType.REMOVE
          || !(filter instanceof EqualFilter equal)
          || !"value".equalsIgnoreCase(equal.getAttributePath().toString())) {
        return Optional.empty();
      }
      String value = equal.getComparisonValue().asText();
      return Optional.of(group -> withoutMember(group, adminAdded, value));
    }
    // The SCIM library reads a value that is not an array as no members: a replace would empty
    // the group.
    if (operation.getOpType() != PatchOpType.REMOVE && !operation.getJsonNode().isArray()) {
      throw new BadRequestException("members needs an array value.");
    }
    return switch (operation.getOpType()) {
      case ADD -> {
        Set<UUID> added = memberIds(operation.getValues(Member.class));
        yield Optional.of(
            group -> {
              Set<UUID> members = new LinkedHashSet<>(group.getMemberIds(adminAdded));
              members.addAll(added);
              return group.withMemberIds(adminAdded, members);
            });
      }
      case REPLACE -> {
        Set<UUID> replacement = memberIds(operation.getValues(Member.class));
        yield Optional.of(group -> group.withMemberIds(adminAdded, replacement));
      }
      // The SCIM library drops the value of a remove, so a remove without a filter would read as
      // "remove every member" whatever the caller listed. Refuse it rather than guess.
      case REMOVE -> Optional.empty();
    };
  }

  /** Removing someone who is not a member changes nothing, so the id need not be valid. */
  private static Group withoutMember(Group group, boolean adminAdded, String userId) {
    Set<UUID> members = new LinkedHashSet<>(group.getMemberIds(adminAdded));
    members.removeIf(memberId -> memberId.toString().equalsIgnoreCase(userId));
    return group.withMemberIds(adminAdded, members);
  }

  private static String requireDisplayName(String displayName) {
    if (displayName == null || displayName.isBlank()) {
      throw new Scim2RuntimeException(new BadRequestException("displayName is required."));
    }
    return checkLength(DISPLAY_NAME, displayName);
  }

  /** Refuses a value the column cannot hold, which the database would report as a 500. */
  private static String checkLength(String attribute, String value) {
    if (value != null && value.length() > MAX_LENGTH) {
      throw new Scim2RuntimeException(
          new BadRequestException(attribute + " is longer than " + MAX_LENGTH + " characters."));
    }
    return value;
  }

  /** A member's value is a user id; one that is not even a UUID cannot be a user. */
  private static Set<UUID> memberIds(List<Member> members) {
    Set<UUID> memberIds = new LinkedHashSet<>();
    for (Member member : members == null ? List.<Member>of() : members) {
      try {
        memberIds.add(UUID.fromString(member.getValue()));
      } catch (IllegalArgumentException | NullPointerException e) {
        throw new Scim2RuntimeException(
            new BadRequestException(
                "Member is not an existing user: " + (member == null ? null : member.getValue())));
      }
    }
    return memberIds;
  }

  /** Runs a repository call, answering its errors as SCIM errors. */
  private static <T> T scim(Supplier<T> call) {
    try {
      return call.get();
    } catch (BaseException e) {
      ScimException scimException =
          switch (e.getErrorCode()) {
            case ALREADY_EXISTS -> ResourceConflictException.uniqueness(e.getMessage());
            case NOT_FOUND -> new ResourceNotFoundException(e.getMessage());
            default ->
                e.getErrorCode().getHttpStatus().isServerError()
                    ? new ServerErrorException(e.getMessage(), null, e)
                    : new BadRequestException(e.getMessage());
          };
      throw new Scim2RuntimeException(scimException);
    }
  }

  private Filter parseFilter(String filter) {
    try {
      return Parser.parseFilter(filter);
    } catch (BadRequestException e) {
      throw new Scim2RuntimeException(e);
    }
  }

  private boolean match(FilterEvaluator filterEvaluator, Filter groupFilter, GroupResource group) {
    try {
      return groupFilter == null
          || groupFilter.visit(filterEvaluator, group.asGenericScimResource().getObjectNode());
    } catch (ScimException e) {
      throw new Scim2RuntimeException(e);
    }
  }
}
