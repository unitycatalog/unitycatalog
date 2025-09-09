import React from 'react';
import { 
  Table, 
  Button, 
  Space, 
  Tag, 
  Typography, 
  Popconfirm,
  Tooltip
} from 'antd';
import { DeleteOutlined, ExclamationCircleOutlined } from '@ant-design/icons';
import { useListTokens, useRevokeToken } from '../../hooks/tokens';
import { TokenInfo } from '../../types/tokens';

const { Text } = Typography;

export default function TokenList() {
  const { data: tokensData, isLoading } = useListTokens();
  const revokeTokenMutation = useRevokeToken();

  const handleRevoke = (tokenId: string) => {
    revokeTokenMutation.mutate(tokenId);
  };

  const getStatusTag = (status: TokenInfo['status'], expiresAt: string) => {
    const now = new Date();
    const expires = new Date(expiresAt);
    
    if (status === 'REVOKED') {
      return <Tag color="red">REVOKED</Tag>;
    }
    
    if (status === 'EXPIRED' || expires <= now) {
      return <Tag color="orange">EXPIRED</Tag>;
    }
    
    return <Tag color="green">ACTIVE</Tag>;
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleString();
  };

  const columns = [
    {
      title: 'Description',
      dataIndex: 'comment',
      key: 'comment',
      render: (comment: string) => comment || <Text type="secondary">No description</Text>,
    },
    {
      title: 'Status',
      key: 'status',
      render: (record: TokenInfo) => getStatusTag(record.status, record.expiresAt),
    },
    {
      title: 'Created',
      dataIndex: 'createdAt',
      key: 'createdAt',
      render: formatDate,
    },
    {
      title: 'Last Used',
      dataIndex: 'lastUsedAt',
      key: 'lastUsedAt',
      render: (lastUsedAt: string) => 
        lastUsedAt ? formatDate(lastUsedAt) : <Text type="secondary">Never</Text>,
    },
    {
      title: 'Expires',
      dataIndex: 'expiresAt',
      key: 'expiresAt',
      render: (expiresAt: string) => {
        const expires = new Date(expiresAt);
        const now = new Date();
        const isExpired = expires <= now;
        
        return (
          <Text type={isExpired ? 'secondary' : undefined}>
            {formatDate(expiresAt)}
          </Text>
        );
      },
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (record: TokenInfo) => {
        const canRevoke = record.status === 'ACTIVE';
        
        return (
          <Space>
            {canRevoke ? (
              <Popconfirm
                title="Revoke Token"
                description="Are you sure you want to revoke this token? This action cannot be undone."
                onConfirm={() => handleRevoke(record.tokenId)}
                okText="Revoke"
                cancelText="Cancel"
                icon={<ExclamationCircleOutlined style={{ color: 'red' }} />}
              >
                <Button 
                  type="text" 
                  danger 
                  icon={<DeleteOutlined />}
                  loading={revokeTokenMutation.isPending}
                >
                  Revoke
                </Button>
              </Popconfirm>
            ) : (
              <Tooltip title="Cannot revoke expired or already revoked tokens">
                <Button 
                  type="text" 
                  danger 
                  icon={<DeleteOutlined />}
                  disabled
                >
                  Revoke
                </Button>
              </Tooltip>
            )}
          </Space>
        );
      },
    },
  ];

  return (
    <Table
      dataSource={tokensData?.tokens || []}
      columns={columns}
      loading={isLoading}
      rowKey="tokenId"
      pagination={{ 
        pageSize: 10,
        showSizeChanger: true,
        showQuickJumper: true,
      }}
      locale={{
        emptyText: 'No tokens found. Create your first token above.',
      }}
    />
  );
}
