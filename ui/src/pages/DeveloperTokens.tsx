import React from 'react';
import { 
  Card, 
  Typography, 
  Space, 
  Alert,
  Row,
  Col
} from 'antd';
import { SafetyOutlined, InfoCircleOutlined } from '@ant-design/icons';
import CreateTokenForm from '../components/tokens/CreateTokenForm';
import TokenList from '../components/tokens/TokenList';

const { Title, Paragraph, Text } = Typography;

export default function DeveloperTokens() {
  return (
    <div style={{ padding: '24px' }}>
      <div style={{ marginBottom: 24 }}>
        <Title level={2}>
          <SafetyOutlined style={{ marginRight: 8 }} />
          Developer Tokens
        </Title>
        <Paragraph type="secondary">
          Create and manage personal access tokens for API authentication. 
          Tokens inherit your current permissions and provide secure access to Unity Catalog APIs.
        </Paragraph>
      </div>

      <Alert
        message="Token Security Notice"
        description={
          <Space direction="vertical" size="small">
            <Text>• Tokens adopt your current permissions; contact an admin for changes</Text>
            <Text>• Token values are displayed only once during creation</Text>
            <Text>• Store tokens securely and never commit them to version control</Text>
            <Text>• Revoke unused tokens promptly</Text>
          </Space>
        }
        type="info"
        icon={<InfoCircleOutlined />}
        style={{ marginBottom: 24 }}
      />

      <Row gutter={[24, 24]}>
        <Col xs={24} lg={8}>
          <Card title="Create New Token" size="small">
            <CreateTokenForm />
          </Card>
        </Col>
        
        <Col xs={24} lg={16}>
          <Card title="Your Tokens" size="small">
            <TokenList />
          </Card>
        </Col>
      </Row>
    </div>
  );
}
