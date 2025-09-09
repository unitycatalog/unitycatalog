import React from 'react';
import { Card, Descriptions, Typography, Alert, Button, Space, Divider, Input, message } from 'antd';
import { CopyOutlined } from '@ant-design/icons';
import { useMsalAuth } from '../context/msal-auth-context';
import { useAuth } from '../context/auth-context';
import { useLoginWithToken } from '../hooks/user';
import { GrantType, TokenType } from '../types/api/control.gen';

const { Title } = Typography;
const { TextArea } = Input;

// Only show in development
const isDevelopment = process.env.NODE_ENV === 'development';

export function AuthDebug() {
  const { account, getIdToken, logout } = useMsalAuth();
  const { currentUser } = useAuth();
  const loginWithTokenMutation = useLoginWithToken();
  const [idToken, setIdToken] = React.useState<string | null>(null);
  const [ucTokenInfo, setUcTokenInfo] = React.useState<any>(null);
  const [isLoadingUcToken, setIsLoadingUcToken] = React.useState(false);

  // Don't render in production
  if (!isDevelopment) {
    return (
      <Alert
        type="error"
        message="Debug page not available in production"
        showIcon
      />
    );
  }

  const handleGetToken = async () => {
    try {
      const token = await getIdToken();
      setIdToken(token);
      console.log('Azure AD ID Token (full):', token);
    } catch (error) {
      console.error('Failed to get Azure ID token:', error);
    }
  };

  const handleGetUcToken = async () => {
    try {
      setIsLoadingUcToken(true);
      const azureToken = await getIdToken();
      if (!azureToken) {
        throw new Error('No Azure ID token available');
      }

      const result = await new Promise((resolve, reject) => {
        loginWithTokenMutation.mutate(
          {
            grant_type: GrantType.TOKEN_EXCHANGE,
            requested_token_type: TokenType.ACCESS_TOKEN,
            subject_token_type: TokenType.ID_TOKEN,
            subject_token: azureToken,
          },
          {
            onSuccess: (data) => {
              console.log('Unity Catalog Token Exchange Response (full):', data);
              console.log('Unity Catalog Access Token (full):', data.access_token);
              setUcTokenInfo(data);
              resolve(data);
            },
            onError: (error) => {
              console.error('Unity Catalog token exchange failed:', error);
              reject(error);
            },
          }
        );
      });
    } catch (error) {
      console.error('Failed to get Unity Catalog token:', error);
    } finally {
      setIsLoadingUcToken(false);
    }
  };

  const copyToClipboard = async (text: string, label: string) => {
    try {
      await navigator.clipboard.writeText(text);
      message.success(`${label} copied to clipboard!`);
    } catch (error) {
      message.error(`Failed to copy ${label}`);
      console.error('Copy failed:', error);
    }
  };

  return (
    <div style={{ padding: 24, maxWidth: 800, margin: '0 auto' }}>
      <Title level={2}>Authentication Debug</Title>
      
      <Card title="Azure AD Account" style={{ marginBottom: 16 }}>
        {account ? (
          <Descriptions column={1} bordered>
            <Descriptions.Item label="UPN">{account.username}</Descriptions.Item>
            <Descriptions.Item label="Name">{account.name}</Descriptions.Item>
            <Descriptions.Item label="Local Account ID">{account.localAccountId}</Descriptions.Item>
            <Descriptions.Item label="Tenant ID">{account.tenantId}</Descriptions.Item>
          </Descriptions>
        ) : (
          <Alert type="warning" message="No Azure AD account found" />
        )}
      </Card>

      <Card title="Unity Catalog User" style={{ marginBottom: 16 }}>
        {currentUser ? (
          <Descriptions column={1} bordered>
            <Descriptions.Item label="ID">{currentUser.id}</Descriptions.Item>
            <Descriptions.Item label="Display Name">{currentUser.displayName}</Descriptions.Item>
            <Descriptions.Item label="Email">{currentUser.emails?.[0]?.value}</Descriptions.Item>
            <Descriptions.Item label="Created At">{currentUser.meta?.created}</Descriptions.Item>
            <Descriptions.Item label="Active">{currentUser.active ? 'Yes' : 'No'}</Descriptions.Item>
          </Descriptions>
        ) : (
          <Alert type="warning" message="No Unity Catalog user found" />
        )}
      </Card>

      {ucTokenInfo && (
        <Card title="Unity Catalog Token Info" style={{ marginBottom: 16 }}>
          <Descriptions column={1} bordered style={{ marginBottom: 16 }}>
            <Descriptions.Item label="Token Type">{ucTokenInfo.token_type}</Descriptions.Item>
            <Descriptions.Item label="Expires In">{ucTokenInfo.expires_in} seconds</Descriptions.Item>
            <Descriptions.Item label="Scope">{ucTokenInfo.scope || 'N/A'}</Descriptions.Item>
          </Descriptions>
          
          <div style={{ marginBottom: 16 }}>
            <strong>Unity Catalog Access Token:</strong>
            <div style={{ marginTop: 8, marginBottom: 8 }}>
              <TextArea
                value={ucTokenInfo.access_token}
                readOnly
                rows={4}
                style={{ fontFamily: 'monospace', fontSize: '12px' }}
              />
            </div>
            <Button 
              size="small" 
              icon={<CopyOutlined />}
              onClick={() => copyToClipboard(ucTokenInfo.access_token, 'Unity Catalog token')}
            >
              Copy UC Token
            </Button>
          </div>
        </Card>
      )}

      {idToken && (
        <Card title="Azure AD Token" style={{ marginBottom: 16 }}>
          <div style={{ marginBottom: 16 }}>
            <strong>Azure AD ID Token:</strong>
            <div style={{ marginTop: 8, marginBottom: 8 }}>
              <TextArea
                value={idToken}
                readOnly
                rows={4}
                style={{ fontFamily: 'monospace', fontSize: '12px' }}
              />
            </div>
            <Button 
              size="small" 
              icon={<CopyOutlined />}
              onClick={() => copyToClipboard(idToken, 'Azure AD token')}
            >
              Copy Azure Token
            </Button>
          </div>
        </Card>
      )}

      <Space>
        <Button onClick={handleGetToken} type="primary">
          Get Azure ID Token (Console)
        </Button>
        
        <Button 
          onClick={handleGetUcToken} 
          type="default"
          loading={isLoadingUcToken}
        >
          Get Unity Catalog Token (Console)
        </Button>
        
        <Button onClick={logout} type="default">
          Logout
        </Button>
      </Space>
    </div>
  );
}
