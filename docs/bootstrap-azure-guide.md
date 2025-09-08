# Azure Bootstrap Guide for Unity Catalog

This guide walks through the complete process for first-time users to bootstrap Unity Catalog with Azure AD authentication and obtain access tokens for CLI/API usage.

## Prerequisites

Before starting, ensure you have:

1. **Azure AD Application configured** with:
   - Client ID and tenant ID
   - Redirect URI: `http://localhost:8080/api/1.0/unity-control/auth/azure-login/callback`
   - Required permissions for OpenID Connect (openid, profile, email scopes)

2. **Unity Catalog server configured** with Azure settings in your configuration file

3. **Tools installed**:
   - `curl` for API calls
   - `jq` for JSON processing (recommended)
   - Unity Catalog CLI (`bin/uc`)

## Step-by-Step Bootstrap Process

### Step 1: Start Unity Catalog Server

Start the Unity Catalog server with bootstrap enabled:

```bash
# Start the server (ensure bootstrap.enabled=true in your config)
bin/start-uc-server
```

The server should be running on `http://localhost:8080` by default.

### Step 2: Initiate Azure OAuth2 Flow

Create a new OAuth2 session to get an Azure authorization URL:

```bash
curl -X POST http://localhost:8080/api/1.0/unity-control/auth/azure-login/start \
  -H "Content-Type: application/json" \
  -d "{}"
```

**Expected Response:**
```json
{
  "session_id": "sess_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "authorization_url": "https://login.microsoftonline.com/YOUR-TENANT-ID/oauth2/v2.0/authorize?response_type=code&client_id=YOUR-CLIENT-ID&redirect_uri=http%3A%2F%2Flocalhost%3A8080%2Fapi%2F1.0%2Funity-control%2Fauth%2Fazure-login%2Fcallback&scope=openid+profile+email&state=STATE:SESSION_ID"
}
```

**Pro Tip:** Use `jq` to extract just the authorization URL:
```bash
curl -X POST http://localhost:8080/api/1.0/unity-control/auth/azure-login/start \
  -H "Content-Type: application/json" \
  -d "{}" | jq -r '.authorization_url'
```

### Step 3: Complete Azure Authentication

1. **Copy the authorization URL** from the response
2. **Open the URL in your web browser**
3. **Sign in with your Azure AD credentials**
4. **Complete any multi-factor authentication** if required
5. **Grant consent** to the application if prompted

After successful authentication, Azure will redirect back to Unity Catalog with an authorization code.

### Step 4: Retrieve Azure JWT Token

The redirect will land on the Unity Catalog callback endpoint, which will return your Azure JWT token:

```
http://localhost:8080/api/1.0/unity-control/auth/azure-login/callback?code=AUTHORIZATION_CODE&state=STATE:SESSION_ID
```

**Expected Response:**
```json
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6IkpZaEFjVFBNWl9MWDZEQmxPV1E3SG4wTmVYRSJ9...",
  "token_type": "Bearer",
  "expires_in": 3599
}
```

**Save this Azure JWT token** - you'll need it for the next step.

### Step 5: Exchange Azure Token for Unity Catalog Token

Use the Azure JWT token to obtain a Unity Catalog access token with OWNER privileges:

```bash
curl -X POST http://localhost:8080/api/1.0/unity-control/auth/bootstrap/token-exchange \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_AZURE_JWT_TOKEN"
```

Replace `YOUR_AZURE_JWT_TOKEN` with the actual token from Step 4.

**Note**: During the bootstrap window, this endpoint automatically grants OWNER privileges equivalent to the legacy local admin user.

**Expected Response:**
```json
{
  "access_token": "eyJraWQiOiJkZWQ5MGQ2MjYwZDQwNzEzNDI0MjczYTA2Y2UxOTNiNTkyMzk5YzdhNTRiMGI1YmRjYzc5OTQ4OTRiOTEyZTA3IiwiYWxnIjoiUlM1MTIiLCJ0eXAiOiJKV1QifQ...",
  "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
  "token_type": "Bearer"
}
```

**Save this Unity Catalog access token** - this is what you'll use for CLI and API operations.

### Step 6: Verify Authentication

Test your Unity Catalog access token with the CLI:

```bash
bin/uc user list --auth_token YOUR_UNITY_CATALOG_ACCESS_TOKEN
```

**Expected Output:**
```
┌────────────────────────────────────┬─────────────┬───────┬────────────────────────────────────┬───────┬───────┬───────┬───────┐
│                 ID                 │    NAME     │ EMAIL │            EXTERNAL_ID             │ STATE │PICTURE│CREATED│UPDATED│
│                                    │             │       │                                    │       │ _URL  │  _AT  │  _AT  │
├────────────────────────────────────┼─────────────┼───────┼────────────────────────────────────┼───────┼───────┼───────┼───────┤
│adfc9626-c4a5-4e7b-b47f-92a96fb70466│Your Name    │your...│624aeef1-9cc7-4a9f-8a8f-c9779303e0fa│ENABLED│null   │1757...│1757...│
└────────────────────────────────────┴─────────────┴───────┴────────────────────────────────────┴───────┴───────┴───────┴───────┘
```

## What Happens During Bootstrap

During the bootstrap process:

1. **Session Management**: Unity Catalog creates a temporary session to track your OAuth2 flow
2. **Azure Authentication**: You authenticate with Azure AD and receive a JWT token
3. **User Creation**: If you don't exist in Unity Catalog, a new user record is created using your Azure profile
4. **OWNER Privileges Grant**: During the bootstrap window, you are automatically granted OWNER privileges equivalent to the legacy local admin
5. **Token Exchange**: Your Azure JWT is validated and exchanged for a Unity Catalog access token with OWNER privileges
6. **Authorization**: You can now use Unity Catalog APIs and CLI with full administrative access

## Important Notes

### Bootstrap Window
- Bootstrap functionality may be time-limited or require special configuration
- Check your `bootstrap.enabled` setting in the Unity Catalog configuration
- Domain restrictions may apply via `bootstrap.allowedDomains` setting

### Token Management
- **Azure JWT tokens** are short-lived (typically 1 hour)
- **Unity Catalog tokens** may have different expiration settings
- Save your Unity Catalog token securely for ongoing use

### Security Considerations
- Never share or log your JWT tokens
- Use HTTPS in production environments
- The redirect URI must match exactly what's configured in Azure AD

## Troubleshooting

### Common Issues

**404 Not Found on token exchange:**
- Verify the URL path: `/api/1.0/unity-control/auth/bootstrap/token-exchange`
- Ensure bootstrap is enabled in server configuration

**400 Bad Request on token exchange:**
- Check that Azure token is in the Authorization header: `Authorization: Bearer TOKEN`
- Verify token format and ensure it's not expired

**Authentication failures:**
- Verify Azure AD application configuration
- Check redirect URI matches exactly
- Ensure required scopes (openid, profile, email) are granted

**CLI auth token issues:**
- Use `--auth_token` (underscore) not `--auth-token` (dash)
- Ensure token is not truncated when copying

## Example Complete Workflow

Here's a complete example with real commands:

```bash
# 1. Start server
bin/start-uc-server

# 2. Get authorization URL
AUTH_URL=$(curl -s -X POST http://localhost:8080/api/1.0/unity-control/auth/azure-login/start \
  -H "Content-Type: application/json" \
  -d "{}" | jq -r '.authorization_url')

echo "Visit this URL to authenticate: $AUTH_URL"

# 3. After browser authentication, you'll get an Azure token
# 4. Exchange it for UC token (replace AZURE_TOKEN with actual token)
UC_TOKEN=$(curl -s -X POST http://localhost:8080/api/1.0/unity-control/auth/bootstrap/token-exchange \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer AZURE_TOKEN" | jq -r '.access_token')

# 5. Use UC token with CLI
bin/uc user list --auth_token "$UC_TOKEN"
```

## Next Steps

After successful bootstrap:

1. **Save your Unity Catalog token** for ongoing use
2. **Explore Unity Catalog features** using the CLI or APIs
3. **Set up additional users** through the same bootstrap process
4. **Configure catalogs, schemas, and tables** as needed
5. **Consider setting up Personal Access Tokens (PATs)** for long-term automation

For more information, see:
- [Unity Catalog CLI Documentation](../README.md)
- [API Reference](../api/README.md)
- [Server Configuration Guide](deployment.md)
