import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Button, Alert, Typography, Card, Spin } from 'antd';
import { CheckCircleOutlined, WarningOutlined, InfoCircleOutlined } from '@ant-design/icons';

const { Title, Paragraph, Text } = Typography;

interface BootstrapStatus {
  bootstrapEnabled: boolean;
  hasAzureAdmin: boolean;
  allowedDomains?: string[];
}

// TODO: Replace with actual API implementation
const bootstrapApi = {
  getBootstrapStatus: async (): Promise<{ data: BootstrapStatus }> => {
    // Simulate API call
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve({
          data: {
            bootstrapEnabled: true,
            hasAzureAdmin: false,
            allowedDomains: ['example.com', 'yourcompany.com']
          }
        });
      }, 1000);
    });
  },
  claimAdmin: async (): Promise<void> => {
    // Simulate API call
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        // For demo purposes, randomly succeed or fail
        if (Math.random() > 0.3) {
          resolve();
        } else {
          reject(new Error('Demo: Failed to claim admin privileges'));
        }
      }, 2000);
    });
  }
};

export const BootstrapAdmin: React.FC = () => {
  const navigate = useNavigate();
  const [status, setStatus] = useState<BootstrapStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  useEffect(() => {
    fetchBootstrapStatus();
  }, []);

  const fetchBootstrapStatus = async () => {
    try {
      setLoading(true);
      const response = await bootstrapApi.getBootstrapStatus();
      setStatus(response.data);
    } catch (err: any) {
      setError(err.response?.data?.message || 'Failed to fetch bootstrap status');
    } finally {
      setLoading(false);
    }
  };

  const handleClaimAdmin = async () => {
    try {
      setLoading(true);
      setError(null);
      await bootstrapApi.claimAdmin();
      setSuccess(true);
    } catch (err: any) {
      setError(err.response?.data?.message || err.message || 'Failed to claim admin privileges');
    } finally {
      setLoading(false);
    }
  };

  if (loading && !status) {
    return (
      <div style={{ padding: 24, textAlign: 'center' }}>
        <Spin size="large" />
        <div style={{ marginTop: 16 }}>
          <Text>Loading bootstrap status...</Text>
        </div>
      </div>
    );
  }

  if (success) {
    return (
      <div style={{ padding: 24, maxWidth: 600, margin: '0 auto' }}>
        <Alert
          type="success"
          message="Admin Privileges Claimed Successfully!"
          description={
            <div>
              <Paragraph>
                You now have administrative access to Unity Catalog.
              </Paragraph>
              <Paragraph>
                <strong>Next steps:</strong>
              </Paragraph>
              <ul>
                <li>Visit the <a href="/admin">Admin Panel</a> to manage users and permissions</li>
                <li>Use the CLI: <code>bin/uc auth login --output jsonPretty</code></li>
              </ul>
            </div>
          }
          showIcon
          icon={<CheckCircleOutlined />}
        />
        <div style={{ marginTop: 24, textAlign: 'center' }}>
          <Button 
            type="primary" 
            size="large"
            onClick={() => navigate('/admin')}
            style={{ marginRight: 8 }}
          >
            Go to Admin Panel
          </Button>
          <Button 
            size="large"
            onClick={() => navigate('/')}
          >
            Continue to Catalogs
          </Button>
        </div>
      </div>
    );
  }

  if (!status?.bootstrapEnabled) {
    return (
      <div style={{ padding: 24, maxWidth: 600, margin: '0 auto' }}>
        <Alert
          type="warning"
          message="Bootstrap Not Enabled"
          description="Bootstrap functionality is not enabled on this Unity Catalog server. Contact your system administrator to enable bootstrap mode."
          showIcon
          icon={<WarningOutlined />}
        />
      </div>
    );
  }

  if (status.hasAzureAdmin) {
    return (
      <div style={{ padding: 24, maxWidth: 600, margin: '0 auto' }}>
        <Alert
          type="info"
          message="Admin Already Configured"
          description={
            <div>
              <Paragraph>
                An Azure admin has already been configured for this Unity Catalog server.
              </Paragraph>
              <Paragraph>
                If you have admin privileges, you can access the <a href="/admin">Admin Panel</a>.
              </Paragraph>
            </div>
          }
          showIcon
          icon={<InfoCircleOutlined />}
        />
      </div>
    );
  }

  return (
    <div style={{ padding: 24, maxWidth: 600, margin: '0 auto' }}>
      <Card>
        <Title level={2}>Claim Admin Privileges</Title>
        <Paragraph>
          This Unity Catalog instance needs an initial administrator. You can claim admin privileges using your Azure AD credentials.
        </Paragraph>
        
        {status.allowedDomains && status.allowedDomains.length > 0 && (
          <Alert
            type="info"
            message="Domain Restrictions"
            description={`Only users from the following domains can claim admin privileges: ${status.allowedDomains.join(', ')}`}
            showIcon
            style={{ marginBottom: 16 }}
          />
        )}
        
        {error && (
          <Alert
            type="error"
            message="Error"
            description={error}
            showIcon
            style={{ marginBottom: 16 }}
          />
        )}
        
        <Button 
          type="primary"
          size="large" 
          onClick={handleClaimAdmin}
          loading={loading}
          block
          style={{ marginTop: 16 }}
        >
          {loading ? 'Claiming Admin Privileges...' : 'Claim Admin Privileges'}
        </Button>
        
        <div style={{ marginTop: 16, textAlign: 'center' }}>
          <Text type="secondary">
            This will grant you OWNER role and administrative access to Unity Catalog
          </Text>
        </div>
      </Card>
    </div>
  );
};
