# Personal Access Token (PAT) Management

This guide explains how to create, manage, and use Personal Access Tokens (PATs) in Unity Catalog for authentication and authorization.

## Overview

Personal Access Tokens provide a secure way to authenticate with Unity Catalog APIs without requiring interactive Azure AD authentication. PATs are:

- **Write-once**: Token values are displayed only once during creation
- **Hashed storage**: Only bcrypt hashes are stored in the database
- **Scoped**: Inherit the permissions of the creating user
- **Time-limited**: Have configurable expiration times
- **Revocable**: Can be revoked by the owner or administrators

## Prerequisites

- Unity Catalog server with PAT functionality enabled
- Valid authentication (Azure AD JWT or existing PAT)
- Appropriate privileges for the desired operations

## Configuration

### Environment Variables

```bash
# Enable PAT functionality
UC_PAT_ENABLED=true

# Default token TTL in minutes (default: 60)
UC_PAT_DEFAULT_TTL_MINUTES=60

# Maximum token TTL in minutes (default: 43200 = 30 days)
UC_PAT_MAX_TTL_MINUTES=43200
```

### Server Properties

```java
// Enable PAT functionality
server.setPatEnabled(true);
server.setPatDefaultTtlMinutes(60);
server.setPatMaxTtlMinutes(43200);
```

## Creating Personal Access Tokens

### Using REST API

Create a new PAT token:

```bash
curl -X POST "http://localhost:8080/api/2.1/unity-catalog/tokens" \
  -H "Authorization: Bearer $AZURE_JWT_OR_PAT" \
  -H "Content-Type: application/json" \
  -d '{
    "comment": "My development token",
    "lifetimeSeconds": 3600
  }'
```

**Response:**
```json
{
  "tokenId": "tok_1234567890abcdef",
  "tokenValue": "pat_1234567890abcdef1234567890abcdef",
  "comment": "My development token",
  "createdAt": "2025-01-15T10:30:00Z",
  "expiresAt": "2025-01-15T11:30:00Z"
}
```

> ⚠️ **Important**: Save the `tokenValue` immediately. It will not be shown again.

### Using Default TTL

If no `lifetimeSeconds` is specified, the default TTL is used:

```bash
curl -X POST "http://localhost:8080/api/2.1/unity-catalog/tokens" \
  -H "Authorization: Bearer $AZURE_JWT_OR_PAT" \
  -H "Content-Type: application/json" \
  -d '{
    "comment": "Token with default TTL"
  }'
```

## Listing Personal Access Tokens

### List Your Tokens

Regular users can list their own tokens:

```bash
curl -X GET "http://localhost:8080/api/2.1/unity-catalog/tokens" \
  -H "Authorization: Bearer $PAT_TOKEN"
```

**Response:**
```json
{
  "tokens": [
    {
      "tokenId": "tok_1234567890abcdef",
      "comment": "My development token",
      "createdAt": "2025-01-15T10:30:00Z",
      "expiresAt": "2025-01-15T11:30:00Z",
      "status": "ACTIVE"
    }
  ]
}
```

Note: `tokenValue` is never returned in list responses for security.

### Admin Token Listing

Administrators can see all tokens across all users:

```bash
curl -X GET "http://localhost:8080/api/2.1/unity-catalog/tokens?includeAll=true" \
  -H "Authorization: Bearer $ADMIN_PAT_TOKEN"
```

## Using Personal Access Tokens

### API Authentication

Use PAT tokens in the `Authorization` header:

```bash
curl -X GET "http://localhost:8080/api/2.1/unity-catalog/catalogs" \
  -H "Authorization: Bearer pat_1234567890abcdef1234567890abcdef"
```

### SDK Usage

**Python SDK:**
```python
from unitycatalog.sdk import ApiClient, CatalogsApi

client = ApiClient()
client.set_default_header('Authorization', 'Bearer pat_1234567890abcdef1234567890abcdef')
catalogs_api = CatalogsApi(client)

catalogs = catalogs_api.list_catalogs()
```

**Java SDK:**
```java
ApiClient client = new ApiClient();
client.addDefaultHeader("Authorization", "Bearer pat_1234567890abcdef1234567890abcdef");
CatalogsApi catalogsApi = new CatalogsApi(client);

ListCatalogsResponse catalogs = catalogsApi.listCatalogs(null, null);
```

## Revoking Personal Access Tokens

### Revoke Your Own Token

```bash
curl -X DELETE "http://localhost:8080/api/2.1/unity-catalog/tokens/tok_1234567890abcdef" \
  -H "Authorization: Bearer $PAT_TOKEN"
```

### Admin Token Revocation

Administrators can revoke any user's tokens:

```bash
curl -X DELETE "http://localhost:8080/api/2.1/unity-catalog/tokens/tok_user_token_id" \
  -H "Authorization: Bearer $ADMIN_PAT_TOKEN"
```

## Token Status and Lifecycle

### Token States

- **ACTIVE**: Token is valid and can be used for authentication
- **EXPIRED**: Token has passed its expiration time
- **REVOKED**: Token has been manually revoked

### Automatic Expiration

Tokens automatically expire based on their `expiresAt` timestamp. Expired tokens are rejected with HTTP 401.

### Status Checking

Check token status by listing tokens and examining the `status` field.

## Security Best Practices

### Token Storage

- **Never commit tokens to version control**
- **Store tokens in secure credential stores** (e.g., Azure Key Vault, AWS Secrets Manager)
- **Use environment variables** for local development
- **Rotate tokens regularly**

### Token Scope

- **Create tokens with minimal required privileges**
- **Use separate tokens for different applications/environments**
- **Revoke unused tokens promptly**

### Monitoring

- **Audit token usage** through server logs
- **Monitor for suspicious authentication patterns**
- **Set up alerts for failed authentication attempts**

## Administration

### Bulk Token Management

Administrators can script token management operations:

```bash
#!/bin/bash
# Revoke all expired tokens
EXPIRED_TOKENS=$(curl -s -H "Authorization: Bearer $ADMIN_PAT" \
  "http://localhost:8080/api/2.1/unity-catalog/tokens?includeAll=true" | \
  jq -r '.tokens[] | select(.status == "EXPIRED") | .tokenId')

for token_id in $EXPIRED_TOKENS; do
  curl -X DELETE "http://localhost:8080/api/2.1/unity-catalog/tokens/$token_id" \
    -H "Authorization: Bearer $ADMIN_PAT"
done
```

### Token Analytics

Query token usage patterns:

```sql
-- Example queries for token analytics (actual schema may vary)
SELECT 
  principal_name,
  COUNT(*) as token_count,
  MAX(created_at) as last_token_created
FROM personal_access_tokens 
WHERE status = 'ACTIVE'
GROUP BY principal_name;

SELECT 
  DATE(created_at) as creation_date,
  COUNT(*) as tokens_created
FROM personal_access_tokens 
GROUP BY DATE(created_at)
ORDER BY creation_date DESC;
```

## Integration Examples

### CI/CD Pipeline

```yaml
# GitHub Actions example
name: Unity Catalog Integration
on: [push]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Test with Unity Catalog
        env:
          UC_PAT_TOKEN: ${{ secrets.UC_PAT_TOKEN }}
        run: |
          curl -H "Authorization: Bearer $UC_PAT_TOKEN" \
            "$UC_SERVER_URL/api/2.1/unity-catalog/catalogs"
```

### Application Configuration

```python
# Python application example
import os
from unitycatalog.sdk import ApiClient, CatalogsApi

def get_unity_catalog_client():
    pat_token = os.environ.get('UC_PAT_TOKEN')
    if not pat_token:
        raise ValueError("UC_PAT_TOKEN environment variable required")
    
    client = ApiClient()
    client.set_default_header('Authorization', f'Bearer {pat_token}')
    return client

# Usage
client = get_unity_catalog_client()
catalogs_api = CatalogsApi(client)
```

## Troubleshooting

### Common Issues

1. **Token not found**
   - Error: HTTP 404 "Token not found"
   - Solution: Verify token ID is correct and you have access

2. **Token expired**
   - Error: HTTP 401 "Token expired"
   - Solution: Create a new token

3. **Insufficient privileges**
   - Error: HTTP 403 "Insufficient privileges"
   - Solution: Ensure your user has the required permissions

4. **Token revoked**
   - Error: HTTP 401 "Token revoked"
   - Solution: Create a new token

### Debugging

Enable debug logging for token authentication:

```bash
UC_LOG_LEVEL=DEBUG
```

Check server logs for detailed authentication and authorization information.

## API Reference

### Create Token

```
POST /api/2.1/unity-catalog/tokens
```

**Request Body:**
```json
{
  "comment": "string",
  "lifetimeSeconds": 3600
}
```

### List Tokens

```
GET /api/2.1/unity-catalog/tokens
```

**Query Parameters:**
- `includeAll`: (admin only) Include tokens from all users

### Revoke Token

```
DELETE /api/2.1/unity-catalog/tokens/{tokenId}
```

**Path Parameters:**
- `tokenId`: The ID of the token to revoke
