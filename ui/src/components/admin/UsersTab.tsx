import React, { useState } from 'react';
import { Table, Button, Space, Modal, Form, Input, message, Popconfirm } from 'antd';
import { PlusOutlined, EditOutlined, DeleteOutlined } from '@ant-design/icons';
import { useListUsers, useCreateUser, useUpdateUser, useDeleteUser } from '../../hooks/admin';
import type { UserInterface } from '../../hooks/user';

interface CreateUserForm {
  email: string;
  displayName: string;
}

export function UsersTab() {
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);
  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [editingUser, setEditingUser] = useState<UserInterface | null>(null);
  const [createForm] = Form.useForm<CreateUserForm>();
  const [editForm] = Form.useForm<CreateUserForm>();

  const { data: users, isLoading, refetch } = useListUsers();
  const createUserMutation = useCreateUser();
  const updateUserMutation = useUpdateUser();
  const deleteUserMutation = useDeleteUser();

  const handleCreateUser = async (values: CreateUserForm) => {
    try {
      await createUserMutation.mutateAsync({
        displayName: values.displayName,
        emails: [{ value: values.email, primary: true }],
        active: true,
        externalId: values.email, // Azure UPN/email as external ID
      });
      message.success('User created successfully');
      setIsCreateModalOpen(false);
      createForm.resetFields();
      refetch();
    } catch (error) {
      message.error('Failed to create user');
    }
  };

  const handleUpdateUser = async (values: CreateUserForm) => {
    if (!editingUser?.id) return;
    
    try {
      await updateUserMutation.mutateAsync({
        id: editingUser.id,
        user: {
          ...editingUser,
          displayName: values.displayName,
          emails: [{ value: values.email, primary: true }],
        },
      });
      message.success('User updated successfully');
      setIsEditModalOpen(false);
      setEditingUser(null);
      editForm.resetFields();
      refetch();
    } catch (error) {
      message.error('Failed to update user');
    }
  };

  const handleDeleteUser = async (userId: string) => {
    try {
      await deleteUserMutation.mutateAsync(userId);
      message.success('User deleted successfully');
      refetch();
    } catch (error) {
      message.error('Failed to delete user');
    }
  };

  const columns = [
    {
      title: 'Email/UPN',
      dataIndex: ['emails', 0, 'value'],
      key: 'email',
    },
    {
      title: 'Display Name',
      dataIndex: 'displayName',
      key: 'displayName',
    },
    {
      title: 'External ID',
      dataIndex: 'externalId',
      key: 'externalId',
    },
    {
      title: 'Status',
      dataIndex: 'active',
      key: 'active',
      render: (active: boolean) => (active ? 'Active' : 'Inactive'),
    },
    {
      title: 'Created',
      dataIndex: ['meta', 'created'],
      key: 'created',
      render: (date: string) => new Date(date).toLocaleDateString(),
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (_: any, record: UserInterface) => (
        <Space>
          <Button
            type="link"
            icon={<EditOutlined />}
            onClick={() => {
              setEditingUser(record);
              editForm.setFieldsValue({
                email: record.emails?.[0]?.value || '',
                displayName: record.displayName || '',
              });
              setIsEditModalOpen(true);
            }}
          >
            Edit
          </Button>
          <Popconfirm
            title="Delete user"
            description="Are you sure you want to delete this user?"
            onConfirm={() => handleDeleteUser(record.id || '')}
            okText="Yes"
            cancelText="No"
          >
            <Button type="link" danger icon={<DeleteOutlined />}>
              Delete
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          onClick={() => setIsCreateModalOpen(true)}
        >
          Create Azure User
        </Button>
      </div>

      <Table
        columns={columns}
        dataSource={users}
        loading={isLoading}
        rowKey="id"
        pagination={{ pageSize: 10 }}
      />

      {/* Create User Modal */}
      <Modal
        title="Create Azure User"
        open={isCreateModalOpen}
        onCancel={() => {
          setIsCreateModalOpen(false);
          createForm.resetFields();
        }}
        footer={null}
      >
        <Form form={createForm} onFinish={handleCreateUser} layout="vertical">
          <Form.Item
            name="email"
            label="Email/UPN"
            rules={[
              { required: true, message: 'Please enter email/UPN' },
              { type: 'email', message: 'Please enter a valid email' },
            ]}
          >
            <Input placeholder="user@domain.com" />
          </Form.Item>
          <Form.Item
            name="displayName"
            label="Display Name"
            rules={[{ required: true, message: 'Please enter display name' }]}
          >
            <Input placeholder="John Doe" />
          </Form.Item>
          <Form.Item>
            <Space>
              <Button type="primary" htmlType="submit" loading={createUserMutation.isPending}>
                Create User
              </Button>
              <Button onClick={() => setIsCreateModalOpen(false)}>
                Cancel
              </Button>
            </Space>
          </Form.Item>
        </Form>
      </Modal>

      {/* Edit User Modal */}
      <Modal
        title="Edit User"
        open={isEditModalOpen}
        onCancel={() => {
          setIsEditModalOpen(false);
          setEditingUser(null);
          editForm.resetFields();
        }}
        footer={null}
      >
        <Form form={editForm} onFinish={handleUpdateUser} layout="vertical">
          <Form.Item
            name="email"
            label="Email/UPN"
            rules={[
              { required: true, message: 'Please enter email/UPN' },
              { type: 'email', message: 'Please enter a valid email' },
            ]}
          >
            <Input placeholder="user@domain.com" />
          </Form.Item>
          <Form.Item
            name="displayName"
            label="Display Name"
            rules={[{ required: true, message: 'Please enter display name' }]}
          >
            <Input placeholder="John Doe" />
          </Form.Item>
          <Form.Item>
            <Space>
              <Button type="primary" htmlType="submit" loading={updateUserMutation.isPending}>
                Update User
              </Button>
              <Button onClick={() => setIsEditModalOpen(false)}>
                Cancel
              </Button>
            </Space>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
