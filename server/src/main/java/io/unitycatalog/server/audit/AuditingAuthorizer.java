package io.unitycatalog.server.audit;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.persist.model.Privileges;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Records each permission change. A grant is recorded when it added a grant. A revoke or a clear is
 * recorded whenever the call returns. In jCasbin 1.99.0, {@code removePolicy} and {@code
 * removeFilteredPolicy} delete the rows through the adapter before they check the in-memory model,
 * so a server that has not reloaded the policy deletes the rows and still returns false. A failed
 * delete throws, so it is not recorded. Public because {@code UnityAccessEvaluator} resolves {@code
 * authorize} on the authorizer's class through a method handle.
 */
public final class AuditingAuthorizer implements UnityCatalogAuthorizer, AutoCloseable {
  private final UnityCatalogAuthorizer delegate;
  private final Auditor auditor;

  AuditingAuthorizer(UnityCatalogAuthorizer delegate, Auditor auditor) {
    this.delegate = delegate;
    this.auditor = auditor;
  }

  @Override
  public boolean grantAuthorization(UUID principal, UUID resource, Privileges action) {
    boolean changed = delegate.grantAuthorization(principal, resource, action);
    if (changed) {
      auditor.recordPermission(AuditRecord.Action.GRANT, principal, resource, action);
    }
    return changed;
  }

  @Override
  public boolean revokeAuthorization(UUID principal, UUID resource, Privileges action) {
    boolean changed = delegate.revokeAuthorization(principal, resource, action);
    auditor.recordPermission(AuditRecord.Action.REVOKE, principal, resource, action);
    return changed;
  }

  @Override
  public boolean clearAuthorizationsForPrincipal(UUID principal) {
    boolean changed = delegate.clearAuthorizationsForPrincipal(principal);
    auditor.recordPermission(AuditRecord.Action.CLEAR_PRINCIPAL_GRANTS, principal, null, null);
    return changed;
  }

  @Override
  public boolean clearAuthorizationsForResource(UUID resource) {
    boolean changed = delegate.clearAuthorizationsForResource(resource);
    auditor.recordPermission(AuditRecord.Action.CLEAR_RESOURCE_GRANTS, null, resource, null);
    return changed;
  }

  @Override
  public boolean addHierarchyChild(UUID parent, UUID child) {
    return delegate.addHierarchyChild(parent, child);
  }

  @Override
  public boolean removeHierarchyChild(UUID parent, UUID child) {
    return delegate.removeHierarchyChild(parent, child);
  }

  @Override
  public boolean removeHierarchyChildren(UUID resource) {
    return delegate.removeHierarchyChildren(resource);
  }

  @Override
  public UUID getHierarchyParent(UUID resource) {
    return delegate.getHierarchyParent(resource);
  }

  @Override
  public boolean authorize(UUID principal, UUID resource, Privileges action) {
    return delegate.authorize(principal, resource, action);
  }

  @Override
  public boolean authorizeAny(UUID principal, UUID resource, Privileges... actions) {
    return delegate.authorizeAny(principal, resource, actions);
  }

  @Override
  public boolean authorizeAll(UUID principal, UUID resource, Privileges... actions) {
    return delegate.authorizeAll(principal, resource, actions);
  }

  @Override
  public List<Privileges> listAuthorizations(UUID principal, UUID resource) {
    return delegate.listAuthorizations(principal, resource);
  }

  @Override
  public Map<UUID, List<Privileges>> listAuthorizations(UUID resource) {
    return delegate.listAuthorizations(resource);
  }

  @Override
  public boolean refreshAuthorizations(long operationStartNanos) {
    return delegate.refreshAuthorizations(operationStartNanos);
  }

  @Override
  public void close() throws Exception {
    if (delegate instanceof AutoCloseable closeable) {
      closeable.close();
    }
  }
}
