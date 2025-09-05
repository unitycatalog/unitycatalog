# Azure AD OIDC Bootstrap Guide

This guide explains how to configure and use the Azure AD OIDC bootstrap functionality to establish an initial OWNER principal in Unity Catalog.

## Overview

The Azure AD OIDC bootstrap feature allows you to:
- Bootstrap an initial OWNER principal using Azure AD authentication
- Eliminate dependency on legacy local admin tokens
- Provide secure, time-windowed initial setup
- Enable Personal Access Token (PAT) management for ongoing operations

## Prerequisites

- Unity Catalog server with Azure AD integration configured
- Valid Azure AD tenant with application registration
- Azure AD user principal that will become the initial OWNER

## Configuration

### Environment Variables

Configure the following environment variables:

```bash
# Enable bootstrap functionality
UC_BOOTSTRAP_ENABLED=true

# Bootstrap window (in minutes, default: 30)
UC_BOOTSTRAP_WINDOW_MINUTES=30

# Azure AD configuration
UC_AZURE_AD_TENANT_ID=your-tenant-id
UC_AZURE_AD_CLIENT_ID=your-client-id

# Initial owner configuration (optional)
UC_BOOTSTRAP_INITIAL_OWNER_UPN=admin@yourdomain.com

# Disable legacy local admin (recommended)
UC_DISABLE_LEGACY_LOCAL_ADMIN=true
```

### Server Properties

For Java-based configuration:

```java
// Enable bootstrap
server.setBootstrapEnabled(true);
server.setBootstrapWindowMinutes(30);

// Azure AD settings
server.setAzureAdTenantId("your-tenant-id");
server.setAzureAdClientId("your-client-id");

// Initial owner
server.setInitialOwnerUpn("admin@yourdomain.com");
```

## Bootstrap Process

### Step 1: Obtain Azure AD JWT Token

Get a valid JWT token from Azure AD for the user who will become the OWNER:

```bash
# Using Azure CLI
az account get-access-token --resource "your-client-id" --query accessToken -o tsv
```

### Step 2: Call Bootstrap Endpoint

Make a POST request to the bootstrap endpoint:

```bash
curl -X POST "http://localhost:8080/api/2.1/unity-catalog/admins/bootstrap-owner" \
  -H "Content-Type: application/json" \
  -d '{
    "azureJwt": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6...",
    "upn": "admin@yourdomain.com"
  }'
```

### Step 3: Verify Bootstrap Success

The response will include the bootstrapped principal information:

```json
{
  "principal": {
    "name": "admin@yourdomain.com",
    "type": "USER"
  },
  "bootstrappedAt": "2025-01-15T10:30:00Z"
}
```

## Security Considerations

### Bootstrap Window

- The bootstrap functionality is only available during the configured window after server startup
- Default window is 30 minutes, configurable via `UC_BOOTSTRAP_WINDOW_MINUTES`
- After the window closes, the endpoint returns HTTP 403 Forbidden

### JWT Validation

- Azure AD JWT tokens are validated against Microsoft's JWKS endpoint
- Token signature, issuer, audience, and expiration are verified
- UPN claim in the JWT must match the UPN in the request

### Idempotent Operations

- Multiple calls with the same UPN return HTTP 200 (success)
- Attempts to bootstrap a different UPN after one exists return HTTP 409 (conflict)
- Bootstrap state is persisted across server restarts

## Post-Bootstrap Operations

### Personal Access Token Management

After bootstrapping, use PAT tokens for ongoing operations:

```bash
# Create a PAT token
curl -X POST "http://localhost:8080/api/2.1/unity-catalog/tokens" \
  -H "Authorization: Bearer $AZURE_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "comment": "My development token",
    "lifetimeSeconds": 3600
  }'
```

### Grant Additional Permissions

Use the bootstrap OWNER to grant permissions to other users:

```bash
# Grant OWNER privilege to another user
curl -X POST "http://localhost:8080/api/2.1/unity-catalog/grants" \
  -H "Authorization: Bearer $PAT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "principal": {
      "name": "user@yourdomain.com",
      "type": "USER"
    },
    "privilege": "OWNER"
  }'
```

## Helm Deployment

For Kubernetes deployments using Helm:

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

The Helm chart includes a post-install hook that automatically performs the bootstrap:

```bash
helm install unity-catalog ./helm \
  --set bootstrap.enabled=true \
  --set bootstrap.initialOwner.upn="admin@yourdomain.com" \
  --set azureAd.tenantId="your-tenant-id" \
  --set azureAd.clientId="your-client-id"
```

## Troubleshooting

### Common Issues

1. **Bootstrap window expired**
   - Error: HTTP 403 "Bootstrap window has expired"
   - Solution: Restart the server or increase `UC_BOOTSTRAP_WINDOW_MINUTES`

2. **Invalid JWT token**
   - Error: HTTP 401 "Invalid Azure JWT token"
   - Solution: Verify token is not expired and has correct audience

3. **UPN mismatch**
   - Error: HTTP 400 "UPN in JWT does not match request UPN"
   - Solution: Ensure the `upn` field matches the UPN claim in the JWT

4. **Owner already exists**
   - Error: HTTP 409 "Bootstrap owner already exists with different UPN"
   - Solution: Use the existing owner UPN or clear the bootstrap state

### Debugging

Enable debug logging to troubleshoot issues:

```bash
UC_LOG_LEVEL=DEBUG
```

Check the server logs for detailed error messages and JWT validation details.

## Migration from Legacy Admin

To migrate from legacy local admin token to Azure AD bootstrap:

1. **Backup existing configuration** and data
2. **Set up Azure AD** integration
3. **Enable bootstrap** with existing admin UPN
4. **Perform bootstrap** during the window
5. **Disable legacy admin** (`UC_DISABLE_LEGACY_LOCAL_ADMIN=true`)
6. **Remove token file** (`etc/conf/token.txt`)
7. **Update deployment scripts** to use PAT tokens

## API Reference

### Bootstrap Owner Endpoint

```
POST /api/2.1/unity-catalog/admins/bootstrap-owner
```

**Request Body:**
```json
{
  "azureJwt": "string",
  "upn": "string"
}
```

**Responses:**
- `200`: Bootstrap successful (idempotent)
- `400`: Invalid request (UPN mismatch, malformed JWT)
- `401`: Authentication failed (invalid JWT)
- `403`: Authorization failed (bootstrap disabled/expired)
- `409`: Conflict (different owner already exists)

**Response Body:**
```json
{
  "principal": {
    "name": "string",
    "type": "USER"
  },
  "bootstrappedAt": "2025-01-15T10:30:00Z"
}
```
