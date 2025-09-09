import React from 'react';
import { Card, Typography, Descriptions, Alert, Button } from 'antd';
import { useAdminStatus } from '../hooks/adminStatus';
import { useBootstrapStatus } from '../hooks/bootstrap';
import { useAuth } from '../context/auth-context';

const { Title, Paragraph } = Typography;

export const AdminStatusDebug: React.FC = () => {
  const { currentUser } = useAuth();
  const { data: adminStatus, isLoading: adminLoading, error: adminError, refetch: refetchAdmin } = useAdminStatus();
  const { data: bootstrapStatus, isLoading: bootstrapLoading, error: bootstrapError, refetch: refetchBootstrap } = useBootstrapStatus();

  return (
    <div style={{ padding: 24, maxWidth: 800, margin: '0 auto' }}>
      <Card>
        <Title level={3}>Admin Status Debug</Title>
        <Paragraph>
          This debug panel shows the current admin privilege detection results.
        </Paragraph>

        <Descriptions title="Current User" bordered size="small" style={{ marginBottom: 24 }}>
          <Descriptions.Item label="User ID" span={3}>
            {currentUser?.id || 'Not logged in'}
          </Descriptions.Item>
          <Descriptions.Item label="Display Name" span={3}>
            {currentUser?.displayName || 'N/A'}
          </Descriptions.Item>
          <Descriptions.Item label="Email" span={3}>
            {currentUser?.emails?.[0]?.value || 'N/A'}
          </Descriptions.Item>
        </Descriptions>

        <Descriptions title="Admin Status" bordered size="small" style={{ marginBottom: 24 }}>
          <Descriptions.Item label="Loading" span={1}>
            {adminLoading ? 'Yes' : 'No'}
          </Descriptions.Item>
          <Descriptions.Item label="Is Admin" span={1}>
            {adminStatus?.isAdmin ? '✅ Yes' : '❌ No'}
          </Descriptions.Item>
          <Descriptions.Item label="Can Manage Users" span={1}>
            {adminStatus?.canManageUsers ? '✅ Yes' : '❌ No'}
          </Descriptions.Item>
          <Descriptions.Item label="Can Manage Permissions" span={3}>
            {adminStatus?.canManagePermissions ? '✅ Yes' : '❌ No'}
          </Descriptions.Item>
          <Descriptions.Item label="Error" span={3}>
            {adminError ? `❌ ${adminError.message}` : '✅ None'}
          </Descriptions.Item>
        </Descriptions>

        <Descriptions title="Bootstrap Status" bordered size="small" style={{ marginBottom: 24 }}>
          <Descriptions.Item label="Loading" span={1}>
            {bootstrapLoading ? 'Yes' : 'No'}
          </Descriptions.Item>
          <Descriptions.Item label="Available" span={1}>
            {bootstrapStatus?.available ? '✅ Yes' : '❌ No'}
          </Descriptions.Item>
          <Descriptions.Item label="Needs Bootstrap" span={1}>
            {bootstrapStatus?.needsBootstrap ? '✅ Yes' : '❌ No'}
          </Descriptions.Item>
          <Descriptions.Item label="Error" span={3}>
            {bootstrapStatus?.error || bootstrapError?.message || '✅ None'}
          </Descriptions.Item>
        </Descriptions>

        <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
          <Button onClick={() => refetchAdmin()}>
            Refresh Admin Status
          </Button>
          <Button onClick={() => refetchBootstrap()}>
            Refresh Bootstrap Status
          </Button>
        </div>

        <Alert
          type="info"
          message="How Admin Detection Works"
          description={
            <div>
              <p><strong>Admin Status:</strong> Tests actual API access by calling:</p>
              <ul>
                <li><code>GET /api/2.1/unity-catalog/scim2/Users</code> (for user management)</li>
                <li><code>GET /api/2.1/unity-catalog/permissions/metastore/unity</code> (for permission management)</li>
              </ul>
              <p><strong>Bootstrap Status:</strong> Tests bootstrap availability by calling:</p>
              <ul>
                <li><code>POST /api/1.0/unity-control/auth/azure-login/start</code></li>
              </ul>
            </div>
          }
        />

        <div style={{ marginTop: 16, padding: 16, backgroundColor: '#f6f6f6', borderRadius: 6 }}>
          <Title level={5}>Raw Data:</Title>
          <pre style={{ fontSize: 12, overflow: 'auto' }}>
            {JSON.stringify({ 
              currentUser: currentUser ? { 
                id: currentUser.id, 
                displayName: currentUser.displayName,
                emails: currentUser.emails 
              } : null,
              adminStatus, 
              bootstrapStatus,
              adminError: adminError?.message,
              bootstrapError: bootstrapError?.message
            }, null, 2)}
          </pre>
        </div>
      </Card>
    </div>
  );
};
