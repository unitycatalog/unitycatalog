# Unity Catalog UI Admin Panel

The Unity Catalog UI Admin Panel provides a web-based interface for managing users and permissions, with integrated support for the Azure bootstrap process for initial admin setup.

## Overview

The Admin Panel offers three key capabilities:

1. **User Management**: Create, view, update, and delete Unity Catalog users via the SCIM2 API
2. **Permissions Management**: Query, grant, and revoke permissions on catalogs, schemas, and tables
3. **Bootstrap Integration**: Seamless integration with the existing Azure bootstrap process for claiming initial admin privileges

## Accessing the Admin Panel

### Prerequisites

- Unity Catalog server running with Azure authentication enabled
- Web browser with access to the Unity Catalog UI (typically `http://localhost:3000`)
- Valid Azure AD authentication

### URLs

- **Admin Panel**: `http://localhost:3000/admin`
- **Admin Status Debug**: `http://localhost:3000/admin/debug` (for troubleshooting)

## Authentication & Authorization

### Admin Privilege Detection

The admin panel automatically detects your administrative privileges by testing access to Unity Catalog APIs:

- **User Management Test**: `GET /api/2.1/unity-catalog/scim2/Users`
- **Permission Management Test**: `GET /api/2.1/unity-catalog/permissions/metastore/unity`

If either test succeeds, you're granted access to the corresponding admin features.

### Access Scenarios

#### ✅ **You Have Admin Privileges**
- Shows full admin panel with enabled tabs based on your specific permissions
- Can manage users and/or permissions immediately

#### 🔄 **Bootstrap Available** 
- Shows bootstrap flow to claim initial admin privileges
- Integrates with existing server bootstrap process
- Redirects through Azure authentication

#### ❌ **No Admin Access**
- Shows access denied message with guidance
- Suggests contacting Unity Catalog administrator

## Admin Panel Features

### User Management Tab

Provides comprehensive user lifecycle management:

#### **View Users**
- Lists all Unity Catalog users with pagination
- Displays user ID, name, email, status, and timestamps
- Real-time data from SCIM2 Users API

#### **Create User**
- Modal form with Azure UPN/email mapping
- Required fields: Display Name, Email, User Name
- Automatic status and metadata generation

#### **Update User**
- Edit user details in modal form
- Modify display name, email, active status
- Preserves user ID and creation metadata

#### **Delete User**
- Confirmation dialog for user deletion
- Permanent removal from Unity Catalog
- Cannot be undone

### Permissions Management Tab

Maps to Unity Catalog CLI permission commands:

#### **Query Permissions**
- Select resource type (catalog, schema, table, etc.)
- Enter resource name (e.g., `my_catalog.my_schema.my_table`)
- Optional principal filter
- View current permissions and grants

#### **Grant Permissions**
- Select resource and privilege type
- Specify principal (user/group/service principal)
- Add permission grants
- Supports all Unity Catalog privilege types

#### **Revoke Permissions**
- Remove specific permission grants
- Select from existing permissions
- Immediate effect on resource access

## Bootstrap Process Integration

### When Bootstrap is Available

If you don't have admin privileges but bootstrap is enabled, the admin panel will show a guided bootstrap flow:

#### **Step 1: Initialize Bootstrap**
- Click "Start Bootstrap Process"
- Calls `/api/1.0/unity-control/auth/azure-login/start`
- Gets Azure authorization URL

#### **Step 2: Azure Authentication**
- Redirects to Azure AD for authentication
- Complete multi-factor authentication if required
- Grant consent to the application

#### **Step 3: Claim Admin Privileges**
- Azure redirects back with authorization code
- Server exchanges Azure JWT for Unity Catalog token with OWNER privileges
- User receives full administrative access

### Post-Bootstrap

After successful bootstrap:
- User has OWNER role equivalent to legacy local admin
- Can access full admin panel immediately
- Can manage other users and permissions
- Bootstrap becomes unavailable for other users (depending on configuration)

## API Integration

### Endpoints Used

The admin panel integrates with these Unity Catalog APIs:

#### **SCIM2 Users API** (`/api/2.1/unity-catalog/scim2/`)
- `GET /Users` - List users with pagination
- `POST /Users` - Create new user
- `PUT /Users/{id}` - Update existing user
- `DELETE /Users/{id}` - Delete user

#### **Permissions API** (`/api/2.1/unity-catalog/permissions/`)
- `GET /permissions/{securable_type}/{full_name}` - Query permissions
- `PATCH /permissions/{securable_type}/{full_name}` - Grant/revoke permissions

#### **Bootstrap API** (`/api/1.0/unity-control/auth/`)
- `POST /azure-login/start` - Initialize Azure OAuth flow
- `GET /azure-login/callback` - Handle Azure redirect
- `POST /bootstrap/token-exchange` - Exchange Azure JWT for UC token

### Authentication Headers

All API calls use the standard Unity Catalog authentication:
- **Authorization**: `Bearer {unity_catalog_token}`
- **Content-Type**: `application/json`

The UI automatically includes the current user's Unity Catalog token in all requests.

## Troubleshooting

### Admin Status Debug Page

Visit `/admin/debug` to diagnose admin privilege issues:

#### **Information Displayed**
- Current user authentication status
- Admin privilege test results
- Bootstrap availability status
- Raw API responses and error messages
- Refresh buttons to re-test

#### **Common Issues**

**"No Admin Access" - Check these:**
- Ensure you're logged in with Azure AD
- Verify your Unity Catalog token is valid
- Confirm your user has OWNER role or admin privileges
- Check if bootstrap is available for new instances

**"Bootstrap Not Available" - Possible causes:**
- Admin already exists (bootstrap disabled)
- Bootstrap not enabled in server configuration
- Domain restrictions prevent your account from claiming admin

**"API Errors" - Troubleshooting steps:**
- Check Unity Catalog server logs
- Verify API endpoints are accessible
- Ensure proper CORS configuration
- Confirm authentication token validity

### Server Configuration

Ensure your Unity Catalog server has proper configuration:

```properties
# Enable Azure authentication
auth.enabled=true
auth.azure.clientId=YOUR_AZURE_CLIENT_ID
auth.azure.tenantId=YOUR_AZURE_TENANT_ID

# Enable bootstrap (for new instances)
bootstrap.enabled=true
bootstrap.allowedDomains=yourcompany.com,example.com
```

## Security Considerations

### Access Control
- Only users with OWNER role can access admin features
- Tab-level permissions based on actual API access
- Real-time privilege validation

### Token Security
- Unity Catalog tokens are never logged or exposed
- Bootstrap process follows OAuth2 best practices
- Azure authentication uses Auth Code + PKCE flow

### Audit Trail
- All admin actions use standard Unity Catalog APIs
- Server-side audit logging captures admin operations
- User management changes are tracked in SCIM2 metadata

## Integration with CLI

The UI admin panel is fully compatible with Unity Catalog CLI:

### **CLI Commands → UI Features**
- `uc user list` → Users tab view
- `uc user create` → Create User modal
- `uc user update` → Edit User modal
- `uc user delete` → Delete User action
- `uc permission get` → Query Permissions
- `uc permission grant` → Grant Permissions
- `uc permission revoke` → Revoke Permissions

### **Shared Data**
- All changes made in UI are immediately visible in CLI
- CLI changes are reflected in UI after refresh
- Same underlying APIs and data models

## Next Steps

After setting up admin access:

1. **Create additional users** through the Users tab
2. **Set up catalog permissions** using the Permissions tab
3. **Configure schemas and tables** through the main UI
4. **Set up automation** using Personal Access Tokens (PATs)
5. **Train team members** on web-based admin workflows

For more information:
- [Unity Catalog CLI Documentation](../README.md)
- [API Reference](../api/README.md)
- [Azure Bootstrap Guide](bootstrap-azure-guide.md)
- [Server Configuration Guide](deployment.md)
