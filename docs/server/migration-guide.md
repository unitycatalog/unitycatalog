# Migration Guide: From Legacy Admin to Azure AD Bootstrap

This guide provides step-by-step instructions for migrating from the legacy local admin token system to the new Azure AD OIDC bootstrap system.

## Overview

The migration moves Unity Catalog from:
- **Legacy**: Local admin token in `etc/conf/token.txt`
- **Modern**: Azure AD authenticated OWNER with PAT token management

## Benefits of Migration

- **Enhanced Security**: No static tokens in containers or file systems
- **Azure Integration**: Leverages existing Azure AD identity infrastructure  
- **Audit Trail**: All authentication events are logged and traceable
- **Scalable Management**: PAT tokens with proper lifecycle management
- **Kubernetes Ready**: Secure deployment without persistent token files

## Prerequisites

- Unity Catalog server version 0.3.0+
- Azure AD tenant with application registration
- Backup of existing Unity Catalog configuration and data
- Administrative access to Unity Catalog deployment

## Migration Steps

### Step 1: Backup Current Configuration

Before starting the migration, backup your current setup:

```bash
# Backup Unity Catalog data
tar -czf unity-catalog-backup-$(date +%Y%m%d).tar.gz \
  etc/conf/ etc/data/ metastore_db/

# Backup current token
cp etc/conf/token.txt etc/conf/token.txt.backup
```

### Step 2: Set Up Azure AD Application

1. **Register Application** in Azure AD:
   ```bash
   az ad app create \
     --display-name "Unity Catalog Bootstrap" \
     --identifier-uris "api://unity-catalog-bootstrap"
   ```

2. **Configure API Permissions** (if required for your setup)

3. **Note the Application (Client) ID** and **Tenant ID**

### Step 3: Identify Migration Principal

Determine which Azure AD user will become the new OWNER:

```bash
# List current grants to identify admin users
curl -H "Authorization: Bearer $(cat etc/conf/token.txt)" \
  "http://localhost:8080/api/2.1/unity-catalog/grants"

# Choose the Azure AD UPN for your admin user
ADMIN_UPN="admin@yourdomain.com"
```

### Step 4: Configure Bootstrap Settings

Update your Unity Catalog configuration:

#### Environment Variables
```bash
# Enable bootstrap
export UC_BOOTSTRAP_ENABLED=true
export UC_BOOTSTRAP_WINDOW_MINUTES=30

# Azure AD configuration  
export UC_AZURE_AD_TENANT_ID="your-tenant-id"
export UC_AZURE_AD_CLIENT_ID="your-client-id"

# Initial owner
export UC_BOOTSTRAP_INITIAL_OWNER_UPN="$ADMIN_UPN"

# Keep legacy admin during migration
export UC_DISABLE_LEGACY_LOCAL_ADMIN=false
```

#### Configuration File (if using application.conf)
```hocon
unitycatalog {
  bootstrap {
    enabled = true
    windowMinutes = 30
    initialOwnerUpn = "admin@yourdomain.com"
  }
  
  azureAd {
    tenantId = "your-tenant-id"
    clientId = "your-client-id"
  }
  
  legacyAdmin {
    disabled = false  # Keep enabled during migration
  }
}
```

### Step 5: Restart Server with Bootstrap Configuration

Restart Unity Catalog server with the new configuration:

```bash
# Stop current server
./bin/uc stop

# Start with bootstrap configuration
./bin/start-uc-server
```

Verify the server starts successfully and the bootstrap endpoint is available:

```bash
# Check server health
curl "http://localhost:8080/api/2.1/unity-catalog/info"

# Verify bootstrap is enabled (should return method details, not 404)
curl -X OPTIONS "http://localhost:8080/api/2.1/unity-catalog/admins/bootstrap-owner"
```

### Step 6: Perform Azure AD Bootstrap

1. **Obtain Azure AD JWT token**:
   ```bash
   # Using Azure CLI
   AZURE_JWT=$(az account get-access-token \
     --resource "your-client-id" \
     --query accessToken -o tsv)
   ```

2. **Call bootstrap endpoint**:
   ```bash
   curl -X POST "http://localhost:8080/api/2.1/unity-catalog/admins/bootstrap-owner" \
     -H "Content-Type: application/json" \
     -d "{
       \"azureJwt\": \"$AZURE_JWT\",
       \"upn\": \"$ADMIN_UPN\"
     }"
   ```

3. **Verify bootstrap success**:
   ```json
   {
     "principal": {
       "name": "admin@yourdomain.com", 
       "type": "USER"
     },
     "bootstrappedAt": "2025-01-15T10:30:00Z"
   }
   ```

### Step 7: Create PAT Token for Admin Operations

Create a PAT token for ongoing administrative operations:

```bash
# Create PAT token using Azure JWT
PAT_RESPONSE=$(curl -X POST "http://localhost:8080/api/2.1/unity-catalog/tokens" \
  -H "Authorization: Bearer $AZURE_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "comment": "Migration admin token",
    "lifetimeSeconds": 86400
  }')

# Extract PAT token value  
PAT_TOKEN=$(echo "$PAT_RESPONSE" | jq -r '.tokenValue')

echo "PAT Token (save this): $PAT_TOKEN"
```

### Step 8: Verify PAT Token Functionality

Test that the PAT token works for administrative operations:

```bash
# List catalogs using PAT
curl -H "Authorization: Bearer $PAT_TOKEN" \
  "http://localhost:8080/api/2.1/unity-catalog/catalogs"

# List grants using PAT  
curl -H "Authorization: Bearer $PAT_TOKEN" \
  "http://localhost:8080/api/2.1/unity-catalog/grants"

# Create a test grant
curl -X POST "http://localhost:8080/api/2.1/unity-catalog/grants" \
  -H "Authorization: Bearer $PAT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "principal": {
      "name": "test@yourdomain.com",
      "type": "USER"  
    },
    "privilege": "USE_CATALOG"
  }'
```

### Step 9: Migrate Client Applications

Update client applications to use PAT tokens instead of the legacy token:

#### Before (Legacy)
```bash
curl -H "Authorization: Bearer $(cat etc/conf/token.txt)" \
  "http://localhost:8080/api/2.1/unity-catalog/catalogs"
```

#### After (PAT)
```bash
curl -H "Authorization: Bearer $PAT_TOKEN" \
  "http://localhost:8080/api/2.1/unity-catalog/catalogs"
```

#### Application Code Updates
```python
# Before
with open('etc/conf/token.txt', 'r') as f:
    token = f.read().strip()

# After  
token = os.environ['UC_PAT_TOKEN']  # Or from secure credential store
```

### Step 10: Disable Legacy Admin

After verifying everything works with PAT tokens:

1. **Update configuration**:
   ```bash
   export UC_DISABLE_LEGACY_LOCAL_ADMIN=true
   ```

2. **Restart server**:
   ```bash
   ./bin/uc stop
   ./bin/start-uc-server
   ```

3. **Verify legacy token is disabled**:
   ```bash
   # This should now fail with 401/403
   curl -H "Authorization: Bearer $(cat etc/conf/token.txt.backup)" \
     "http://localhost:8080/api/2.1/unity-catalog/catalogs"
   ```

4. **Remove token file**:
   ```bash
   rm etc/conf/token.txt
   # Keep backup until migration is fully validated
   ```

### Step 11: Update Deployment Scripts

Update your deployment and automation scripts:

#### Docker Compose
```yaml
# Before
volumes:
  - ./etc/conf/token.txt:/opt/unitycatalog/etc/conf/token.txt

# After (remove volume mount, use environment variables)
environment:
  - UC_BOOTSTRAP_ENABLED=true
  - UC_AZURE_AD_TENANT_ID=your-tenant-id
  - UC_PAT_TOKEN=${UC_PAT_TOKEN}
```

#### Kubernetes Deployment
```yaml
# Before
volumeMounts:
  - name: token-file
    mountPath: /opt/unitycatalog/etc/conf/token.txt

# After (use secrets)
env:
  - name: UC_PAT_TOKEN
    valueFrom:
      secretKeyRef:
        name: unity-catalog-secrets
        key: pat-token
```

## Helm Migration

If using Helm, update your deployment:

### Before (Legacy)
```yaml
# values.yaml
legacyAdmin:
  enabled: true
  tokenFile: "path/to/token.txt"
```

### After (Bootstrap)
```yaml
# values.yaml
bootstrap:
  enabled: true
  windowMinutes: 30
  initialOwner:
    upn: "admin@yourdomain.com"

azureAd:
  tenantId: "your-tenant-id"
  clientId: "your-client-id"

legacyAdmin:
  disabled: true
```

## Validation Checklist

After migration, verify:

- [ ] Azure AD bootstrap completed successfully
- [ ] PAT token created and stored securely
- [ ] All client applications updated to use PAT tokens
- [ ] Legacy token file removed
- [ ] Legacy admin disabled in configuration
- [ ] Deployment scripts updated
- [ ] Backup verified and stored safely
- [ ] All team members informed of new token management process

## Rollback Plan

If issues occur during migration:

1. **Stop the server**
2. **Restore backup configuration**:
   ```bash
   cp etc/conf/token.txt.backup etc/conf/token.txt
   export UC_DISABLE_LEGACY_LOCAL_ADMIN=false
   export UC_BOOTSTRAP_ENABLED=false
   ```
3. **Restart server with legacy configuration**
4. **Investigate issues and retry migration**

## Troubleshooting

### Common Migration Issues

1. **Bootstrap window expired**
   - Restart server to reset the window
   - Consider increasing `UC_BOOTSTRAP_WINDOW_MINUTES`

2. **Azure JWT token invalid**
   - Verify Azure AD configuration
   - Check token expiration
   - Ensure correct audience/scope

3. **UPN mismatch**
   - Verify the UPN in Azure AD matches the request
   - Check for typos in configuration

4. **PAT token creation fails**
   - Verify bootstrap completed successfully
   - Check that Azure JWT is still valid
   - Ensure proper privileges

5. **Client applications fail after migration**
   - Verify PAT token is correctly configured
   - Check that legacy admin is properly disabled
   - Validate network connectivity

### Getting Help

If you encounter issues during migration:

1. **Check server logs** for detailed error messages
2. **Enable debug logging**: `UC_LOG_LEVEL=DEBUG`
3. **Verify Azure AD configuration** in the Azure portal
4. **Test with curl** before updating application code
5. **Consult the Unity Catalog documentation** for additional guidance

## Post-Migration Best Practices

- **Rotate PAT tokens regularly**
- **Use separate tokens for different environments**
- **Store tokens in secure credential stores**
- **Monitor token usage and expiration**
- **Set up automated token rotation** where possible
- **Document the new authentication flow** for your team
