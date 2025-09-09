import React, { useState } from 'react';
import { 
  Button, 
  Form, 
  Input, 
  Select, 
  Modal, 
  Typography, 
  Alert,
  Space,
  message
} from 'antd';
import { CopyOutlined, CheckCircleOutlined } from '@ant-design/icons';
import { useCreateToken } from '../../hooks/tokens';
import { TTL_OPTIONS, MAX_TTL_SECONDS, CreateTokenResponse } from '../../types/tokens';

const { Text } = Typography;

interface CreateTokenFormProps {
  onSuccess?: () => void;
}

export default function CreateTokenForm({ onSuccess }: CreateTokenFormProps) {
  const [form] = Form.useForm();
  const [isModalVisible, setIsModalVisible] = useState(false);
  const [createdToken, setCreatedToken] = useState<CreateTokenResponse | null>(null);
  const [tokenSaved, setTokenSaved] = useState(false);
  const createTokenMutation = useCreateToken();

  const handleSubmit = async (values: { comment: string; lifetimeSeconds: number }) => {
    try {
      // Client-side validation
      if (values.lifetimeSeconds > MAX_TTL_SECONDS) {
        message.error('Token lifetime cannot exceed 60 days');
        return;
      }

      const response = await createTokenMutation.mutateAsync(values);
      setCreatedToken(response);
      setIsModalVisible(true);
      form.resetFields();
      onSuccess?.();
    } catch (error) {
      // Error handling is done in the mutation's onError
    }
  };

  const copyToClipboard = async () => {
    if (createdToken?.tokenValue) {
      await navigator.clipboard.writeText(createdToken.tokenValue);
      message.success('Token copied to clipboard');
    }
  };

  const handleModalClose = () => {
    if (!tokenSaved) {
      Modal.confirm({
        title: 'Are you sure?',
        content: 'This token will never be displayed again. Make sure you have saved it.',
        onOk: () => {
          setIsModalVisible(false);
          setCreatedToken(null);
          setTokenSaved(false);
        },
      });
    } else {
      setIsModalVisible(false);
      setCreatedToken(null);
      setTokenSaved(false);
    }
  };

  return (
    <>
      <Form
        form={form}
        layout="vertical"
        onFinish={handleSubmit}
        initialValues={{ lifetimeSeconds: 86400 }} // Default to 24 hours
      >
        <Form.Item
          name="comment"
          label="Token Description"
          extra="A helpful description to identify this token"
          rules={[
            { required: true, message: 'Please provide a token description' },
            { max: 255, message: 'Description must be 255 characters or less' }
          ]}
        >
          <Input.TextArea 
            placeholder="e.g., Development environment access"
            maxLength={255}
            rows={2}
          />
        </Form.Item>

        <Form.Item
          name="lifetimeSeconds"
          label="Token Lifetime"
          rules={[
            { required: true, message: 'Please select a token lifetime' },
            {
              validator: (_, value) => {
                if (value > MAX_TTL_SECONDS) {
                  return Promise.reject('Token lifetime cannot exceed 60 days');
                }
                return Promise.resolve();
              }
            }
          ]}
        >
          <Select 
            placeholder="Select token lifetime"
            options={TTL_OPTIONS.map(option => ({
              label: option.label,
              value: option.seconds
            }))}
          />
        </Form.Item>

        <Alert
          message="Token Permissions"
          description="This token will inherit your current permissions. Contact an admin to change your access level."
          type="info"
          style={{ marginBottom: 16 }}
        />

        <Form.Item>
          <Button 
            type="primary" 
            htmlType="submit" 
            loading={createTokenMutation.isPending}
            block
          >
            Create Token
          </Button>
        </Form.Item>
      </Form>

      <Modal
        title={
          <Space>
            <CheckCircleOutlined style={{ color: '#52c41a' }} />
            Token Created Successfully
          </Space>
        }
        open={isModalVisible}
        onCancel={handleModalClose}
        closable={false}
        maskClosable={false}
        footer={[
          <Button key="copy" icon={<CopyOutlined />} onClick={copyToClipboard}>
            Copy Token
          </Button>,
          <Button 
            key="saved" 
            type="primary" 
            onClick={() => setTokenSaved(true)}
            disabled={tokenSaved}
          >
            {tokenSaved ? 'Saved!' : 'I have saved it'}
          </Button>,
          <Button 
            key="close" 
            onClick={handleModalClose}
            disabled={!tokenSaved}
          >
            Close
          </Button>,
        ]}
        width={600}
      >
        <Alert
          message="Important: Save your token now"
          description="This token will never be displayed again. Store it securely."
          type="warning"
          style={{ marginBottom: 16 }}
        />
        
        <div style={{ marginBottom: 16 }}>
          <Text strong>Token Value:</Text>
          <div 
            style={{ 
              background: '#f5f5f5', 
              padding: 12, 
              borderRadius: 4, 
              fontFamily: 'monospace',
              wordBreak: 'break-all',
              border: '1px solid #d9d9d9'
            }}
          >
            {createdToken?.tokenValue}
          </div>
        </div>

        {createdToken?.comment && (
          <div style={{ marginBottom: 8 }}>
            <Text strong>Description:</Text> {createdToken.comment}
          </div>
        )}

        <div style={{ marginBottom: 8 }}>
          <Text strong>Expires:</Text> {new Date(createdToken?.expiresAt || '').toLocaleString()}
        </div>
      </Modal>
    </>
  );
}
