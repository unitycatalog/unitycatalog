import React from 'react';
import { Tabs, Alert, Typography, Spin } from 'antd';
import { UserOutlined, SafetyOutlined } from '@ant-design/icons';
import { UsersTab } from '../components/admin/UsersTab';
import { PermissionsTab } from '../components/admin/PermissionsTab';
import { BootstrapFlow } from '../components/BootstrapFlow';
import { useAuth } from '../context/auth-context';
import { useAdminStatus } from '../hooks/adminStatus';
import { useBootstrapStatus } from '../hooks/bootstrap';

const { Title } = Typography;

export function AdminPanel() {
  const { currentUser } = useAuth();
  const { data: adminStatus, isLoading: adminLoading, refetch: refetchAdminStatus } = useAdminStatus();
  const { data: bootstrapStatus, isLoading: bootstrapLoading } = useBootstrapStatus();
  
  // Show loading while checking status
  if (adminLoading || bootstrapLoading) {
    return (
      <div style={{ padding: 24, textAlign: 'center' }}>
        <Spin size="large" />
        <div style={{ marginTop: 16 }}>
          <Typography.Text>Checking admin privileges...</Typography.Text>
        </div>
      </div>
    );
  }
  
  // User has admin privileges - show full admin panel
  if (adminStatus?.isAdmin) {
    const tabItems = [
      {
        key: 'users',
        label: (
          <span>
            <UserOutlined />
            Users
          </span>
        ),
        children: <UsersTab />,
        disabled: !adminStatus.canManageUsers,
      },
      {
        key: 'permissions',
        label: (
          <span>
            <SafetyOutlined />
            Permissions
          </span>
        ),
        children: <PermissionsTab />,
        disabled: !adminStatus.canManagePermissions,
      },
    ];

    return (
      <div style={{ padding: 24, maxWidth: 1200, margin: '0 auto' }}>
        <Title level={2}>Admin Panel</Title>
        <Tabs items={tabItems} defaultActiveKey="users" />
      </div>
    );
  }
  
  // Bootstrap is available - show bootstrap flow
  if (bootstrapStatus?.available && bootstrapStatus?.needsBootstrap) {
    return (
      <BootstrapFlow 
        onComplete={() => {
          // Refresh admin status after successful bootstrap
          refetchAdminStatus();
        }} 
      />
    );
  }

// Helper function to check if user has admin privileges
function checkIsAdmin(user: any): boolean {
  // Check if user has admin/OWNER role in their token claims
  // This should be replaced with proper role checking based on Unity Catalog's auth model
  
  // For now, check if user is in admin role or has specific admin attributes
  // This is a placeholder - in production, this should check:
  // 1. User's role claims from Unity Catalog token
  // 2. Whether user has OWNER privileges on the catalog
  // 3. Or check against a specific admin token/role endpoint
  
  return false; // Temporarily block all users until proper admin checking is implemented
}

  
  // No admin access and no bootstrap available - show access denied
  return (
    <div style={{ padding: 24, maxWidth: 800, margin: '0 auto' }}>
      <Alert
        type="error"
        message="Admin Access Required"
        description={
          <div>
            <p>You need administrative privileges to access this panel.</p>
            <p><strong>To get admin access:</strong></p>
            <ul>
              <li>Contact your Unity Catalog administrator to grant you OWNER role privileges</li>
              <li>If you have an admin token, ensure you're logged in with the correct Azure account</li>
              <li>Check that your Unity Catalog server has proper admin role configuration</li>
            </ul>
            <p><em>Note: Only users with OWNER role or explicit admin tokens can manage users and permissions.</em></p>
            {bootstrapStatus?.error && (
              <p><strong>Bootstrap Status:</strong> {bootstrapStatus.error}</p>
            )}
          </div>
        }
        showIcon
      />
    </div>
  );
}
