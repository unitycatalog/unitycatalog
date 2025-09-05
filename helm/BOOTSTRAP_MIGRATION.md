# Azure AD Bootstrap Migration Guide

## Overview

This guide covers the migration from legacy local admin token to Azure AD-based OWNER bootstrap for Unity Catalog.

## Prerequisites

- Azure AD tenant with appropriate permissions
- Unity Catalog deployed with `bootstrap.enabled: true`
- Azure service principal or managed identity configured

## Configuration

### Option 1: Managed Identity (Recommended for Azure)

```yaml
bootstrap:
  enabled: true
  windowMinutes: 30
  initialOwner:
    upn: "admin@contoso.onmicrosoft.com"

# No additional Azure configuration needed - uses pod managed identity
```

### Option 2: Service Principal

1. Create Azure AD service principal:
```bash
az ad sp create-for-rbac --name "unity-catalog-bootstrap" --role Reader
```

2. Create Kubernetes secret:
```bash
kubectl create secret generic azure-bootstrap-credentials \
  --from-literal=clientId="<client-id>" \
  --from-literal=clientSecret="<client-secret>" \
  --from-literal=tenantId="<tenant-id>"
```

3. Configure values:
```yaml
bootstrap:
  enabled: true
  windowMinutes: 30
  initialOwner:
    upn: "admin@contoso.onmicrosoft.com"
  azure:
    clientSecretName: azure-bootstrap-credentials
```

## Migration Steps

### 1. Enable Bootstrap (New Deployment)

For new deployments, simply set:
```yaml
bootstrap:
  enabled: true
  initialOwner:
    upn: "admin@contoso.onmicrosoft.com"
```

### 2. Migration from Legacy Token

For existing deployments with legacy local admin:

1. **Prepare**: Set bootstrap configuration but keep legacy enabled:
```yaml
bootstrap:
  enabled: true
  initialOwner:
    upn: "admin@contoso.onmicrosoft.com"
  # Legacy token still available for backward compatibility
```

2. **Execute**: Upgrade deployment - bootstrap job will run
3. **Verify**: Check job logs and PAT token secret creation
4. **Complete**: Optional - disable legacy local admin in future release:
```yaml
server:
  config:
    extraProperties:
      bootstrap.disableLegacyLocalAdmin: "true"
```

## Post-Bootstrap Operations

### Retrieve PAT Token

After successful bootstrap, retrieve the PAT token:
```bash
kubectl get secret <release-name>-server-bootstrap-pat-token -o jsonpath='{.data.token}' | base64 -d
```

**Important**: Store this token securely. It will not be displayed again.

### Disable Bootstrap Job

After successful bootstrap, you can disable the job to prevent re-runs:
```yaml
bootstrap:
  enabled: false  # Disable after successful bootstrap
```

### Grant Additional Bootstrap Privileges

To allow other principals to perform bootstrap operations:
```bash
# Using the PAT token from bootstrap
curl -X PATCH "https://unity-catalog.example.com/api/2.1/unity-catalog/permissions/metastore/<metastore-id>" \
  -H "Authorization: Bearer <pat-token>" \
  -H "Content-Type: application/json" \
  -d '{"changes": [{"principal": "other-admin@contoso.com", "add": ["BOOTSTRAP_OWNER"]}]}'
```

## Rollback Procedures

### Re-enable Legacy Local Admin

If you need to revert to legacy token-based authentication:

1. **Immediate Rollback**: Update configuration to re-enable legacy admin:
```yaml
server:
  config:
    extraProperties:
      bootstrap.disableLegacyLocalAdmin: "false"
  jwtKeypairSecret:
    create: true  # Ensures token.txt is available
```

2. **Disable Bootstrap**: Prevent bootstrap job from running:
```yaml
bootstrap:
  enabled: false
```

3. **Redeploy**: Apply the changes and restart the server

### Remove Bootstrap Configuration

To completely remove bootstrap configuration:

1. **Delete secrets**:
```bash
kubectl delete secret <release-name>-server-bootstrap-pat-token
kubectl delete secret azure-bootstrap-credentials  # if using service principal
```

2. **Clean values configuration**:
```yaml
# Remove or comment out bootstrap section
# bootstrap:
#   enabled: false
```

3. **Remove completed jobs**:
```bash
kubectl delete job <release-name>-server-azure-bootstrap
```

## Credential Rotation

### Rotate Azure Service Principal

1. **Create new credentials**:
```bash
az ad sp credential reset --id <app-id>
```

2. **Update Kubernetes secret**:
```bash
kubectl create secret generic azure-bootstrap-credentials-new \
  --from-literal=clientId="<client-id>" \
  --from-literal=clientSecret="<new-client-secret>" \
  --from-literal=tenantId="<tenant-id>"
```

3. **Update values configuration**:
```yaml
bootstrap:
  azure:
    clientSecretName: azure-bootstrap-credentials-new
```

4. **Clean up old secret**:
```bash
kubectl delete secret azure-bootstrap-credentials
```

### Rotate PAT Token

1. **Generate new PAT** using existing OWNER privileges or re-run bootstrap
2. **Update applications** that use the PAT token
3. **Delete old secret**:
```bash
kubectl delete secret <release-name>-server-bootstrap-pat-token
```

## Troubleshooting

### Bootstrap Job Fails

1. Check job logs:
```bash
kubectl logs job/<release-name>-server-azure-bootstrap
```

2. Common issues:
   - Azure AD token acquisition failure: Check service principal credentials or managed identity configuration
   - Unity Catalog server not ready: Job will retry automatically
   - Network connectivity: Verify pod-to-service communication

### Legacy Token Still Required

If you need to re-enable legacy token support:
```yaml
server:
  config:
    extraProperties:
      bootstrap.disableLegacyLocalAdmin: "false"
```

### Bootstrap Window Closed

If bootstrap window has expired:
1. Restart the Unity Catalog server to reset the window
2. Or grant BOOTSTRAP_OWNER privilege to the intended principal

## Security Considerations

- **PAT Token**: Treat as highly sensitive credential
- **Azure Credentials**: Use managed identity when possible
- **Bootstrap Window**: Keep windowMinutes minimal (default: 30)
- **Job Cleanup**: Bootstrap job is automatically cleaned up after completion
- **Credential Rotation**: Establish regular rotation schedule for service principal credentials
