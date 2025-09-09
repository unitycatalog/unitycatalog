import React, { useState } from 'react';
import { Card, Button, Alert, Typography, Steps, Spin } from 'antd';
import { CheckCircleOutlined, LoadingOutlined, CloudOutlined } from '@ant-design/icons';

const { Title, Paragraph } = Typography;
const { Step } = Steps;

interface BootstrapFlowProps {
  onComplete?: () => void;
}

type BootstrapStep = 'start' | 'redirecting' | 'waiting' | 'error';

export const BootstrapFlow: React.FC<BootstrapFlowProps> = ({ onComplete }) => {
  const [currentStep, setCurrentStep] = useState<BootstrapStep>('start');
  const [error, setError] = useState<string>('');
  const [authUrl, setAuthUrl] = useState<string>('');

  const handleStartBootstrap = async () => {
    try {
      setError('');
      setCurrentStep('redirecting');
      
      const response = await fetch('/api/1.0/unity-control/auth/azure-login/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}'
      });
      
      if (!response.ok) {
        throw new Error(`Failed to start bootstrap: ${response.status}`);
      }
      
      const data = await response.json();
      setAuthUrl(data.authorization_url);
      
      // Redirect to Azure authentication
      window.location.href = data.authorization_url;
      
    } catch (err: any) {
      setError(err.message || 'Failed to start bootstrap process');
      setCurrentStep('error');
    }
  };

  const getStepStatus = (step: number) => {
    if (currentStep === 'error') return 'error';
    if (currentStep === 'start' && step === 0) return 'process';
    if (currentStep === 'redirecting' && step <= 1) return 'process';
    if (currentStep === 'waiting' && step <= 2) return 'process';
    return 'wait';
  };

  return (
    <div style={{ padding: 24, maxWidth: 700, margin: '0 auto' }}>
      <Card>
        <div style={{ textAlign: 'center', marginBottom: 32 }}>
          <CloudOutlined style={{ fontSize: 48, color: '#0078d4', marginBottom: 16 }} />
          <Title level={2}>Claim Admin Privileges</Title>
          <Paragraph>
            This Unity Catalog instance needs an initial administrator. 
            You can claim admin privileges using your Azure AD credentials.
          </Paragraph>
        </div>

        <Steps direction="vertical" size="small" style={{ marginBottom: 32 }}>
          <Step
            title="Initialize Bootstrap"
            description="Start the admin claiming process"
            status={getStepStatus(0)}
            icon={currentStep === 'redirecting' && <LoadingOutlined />}
          />
          <Step
            title="Azure Authentication"
            description="Authenticate with your Azure AD account"
            status={getStepStatus(1)}
          />
          <Step
            title="Grant Admin Privileges"
            description="Receive OWNER privileges in Unity Catalog"
            status={getStepStatus(2)}
            icon={currentStep === 'waiting' && <LoadingOutlined />}
          />
        </Steps>

        {currentStep === 'start' && (
          <div style={{ textAlign: 'center' }}>
            <Button
              type="primary"
              size="large"
              icon={<CloudOutlined />}
              onClick={handleStartBootstrap}
              style={{ minWidth: 200 }}
            >
              Start Bootstrap Process
            </Button>
            <div style={{ marginTop: 16, color: '#666' }}>
              <Paragraph type="secondary">
                You will be redirected to Azure AD to authenticate. 
                After successful authentication, you'll be granted OWNER privileges.
              </Paragraph>
            </div>
          </div>
        )}

        {currentStep === 'redirecting' && (
          <div style={{ textAlign: 'center' }}>
            <Spin size="large" />
            <div style={{ marginTop: 16 }}>
              <Paragraph>Redirecting to Azure AD authentication...</Paragraph>
            </div>
          </div>
        )}

        {currentStep === 'waiting' && (
          <div style={{ textAlign: 'center' }}>
            <Spin size="large" />
            <div style={{ marginTop: 16 }}>
              <Paragraph>Processing authentication and granting admin privileges...</Paragraph>
            </div>
          </div>
        )}

        {currentStep === 'error' && (
          <Alert
            type="error"
            message="Bootstrap Failed"
            description={error}
            showIcon
            action={
              <Button size="small" onClick={() => setCurrentStep('start')}>
                Try Again
              </Button>
            }
          />
        )}

        <div style={{ marginTop: 24, padding: 16, backgroundColor: '#f6f6f6', borderRadius: 6 }}>
          <Typography.Text strong>Important Notes:</Typography.Text>
          <ul style={{ marginTop: 8, marginBottom: 0 }}>
            <li>Only users from allowed domains can claim admin privileges</li>
            <li>Bootstrap is typically available only for new Unity Catalog instances</li>
            <li>You will receive OWNER role equivalent to the legacy local admin</li>
          </ul>
        </div>
      </Card>
    </div>
  );
};
