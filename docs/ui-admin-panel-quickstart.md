# Unity Catalog UI Admin Panel - Quick Start Guide

## TL;DR - Getting Started

1. **Visit**: `http://localhost:3000/admin`
2. **If you see the admin panel**: You have admin privileges ✅
3. **If you see bootstrap flow**: Follow prompts to claim admin privileges
4. **If you see access denied**: Contact your Unity Catalog administrator

## Quick Reference

### Admin Panel URLs
- **Admin Panel**: `/admin`
- **Debug Info**: `/admin/debug`
- **Auth Debug**: `/auth/debug`

### Key Features
- **Users Tab**: Manage Unity Catalog users (create/edit/delete)
- **Permissions Tab**: Grant/revoke permissions on catalogs, schemas, tables
- **Bootstrap Integration**: Claim initial admin privileges via Azure

### User Management
```
Create User → Modal form → Display Name + Email + Username
Edit User → Click edit icon → Update details
Delete User → Click delete icon → Confirm deletion
```

### Permission Management
```
Query → Select resource type + name → View current permissions
Grant → Select resource + privilege + principal → Add permission
Revoke → Select existing permission → Remove access
```

### Bootstrap Process
```
No Admin Access → Bootstrap Available → Start Process → Azure Auth → Admin Access
```

### CLI Equivalents
- UI Users Tab ↔ `uc user list/create/update/delete`
- UI Permissions Tab ↔ `uc permission get/grant/revoke`
- UI Bootstrap ↔ Manual bootstrap process from [bootstrap guide](bootstrap-azure-guide.md)

### Troubleshooting
1. **Check `/admin/debug`** for detailed status
2. **Verify Azure authentication** at `/auth/debug`
3. **Check server logs** for API errors
4. **Confirm OWNER role** in Unity Catalog token

### Common Commands (CLI Reference)
```bash
# List users (equivalent to Users tab)
uc user list

# Grant permission (equivalent to Permissions tab)
uc permission grant --securable-type catalog --name my_catalog --principal user@example.com --privilege SELECT

# Check current user privileges
uc auth whoami
```

For complete documentation, see [UI Admin Panel Guide](ui-admin-panel.md).
