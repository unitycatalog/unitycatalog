import React, { useState } from 'react';
import { Form, Select, Input, Button, Table, Space, message, Card } from 'antd';
import { SearchOutlined, PlusOutlined, DeleteOutlined } from '@ant-design/icons';
import { useGetPermissions, useUpdatePermissions } from '../../hooks/admin';
import { SecurableType, Privilege } from '../../types/api/catalog.gen';

interface PermissionForm {
  resourceType: SecurableType;
  resourceName: string;
  principal: string;
  permission: Privilege;
}

const resourceTypes: { label: string; value: SecurableType }[] = [
  { label: 'Metastore', value: SecurableType.metastore },
  { label: 'Catalog', value: SecurableType.catalog },
  { label: 'Schema', value: SecurableType.schema },
  { label: 'Table', value: SecurableType.table },
  { label: 'Function', value: SecurableType.function },
  { label: 'Volume', value: SecurableType.volume },
  { label: 'Registered Model', value: SecurableType.registered_model },
];

const privileges: { label: string; value: Privilege }[] = [
  { label: 'Create Catalog', value: Privilege.CREATE_CATALOG },
  { label: 'Use Catalog', value: Privilege.USE_CATALOG },
  { label: 'Create Schema', value: Privilege.CREATE_SCHEMA },
  { label: 'Use Schema', value: Privilege.USE_SCHEMA },
  { label: 'Create Table', value: Privilege.CREATE_TABLE },
  { label: 'Select', value: Privilege.SELECT },
  { label: 'Modify', value: Privilege.MODIFY },
  { label: 'Create Function', value: Privilege.CREATE_FUNCTION },
  { label: 'Execute', value: Privilege.EXECUTE },
  { label: 'Create Volume', value: Privilege.CREATE_VOLUME },
  { label: 'Read Volume', value: Privilege.READ_VOLUME },
  { label: 'Create Model', value: Privilege.CREATE_MODEL },
];

export function PermissionsTab() {
  const [queryForm] = Form.useForm();
  const [grantForm] = Form.useForm<PermissionForm>();
  const [queryParams, setQueryParams] = useState<{
    resourceType?: SecurableType;
    resourceName?: string;
    principal?: string;
  }>({});

  const { data: permissions, isLoading, refetch } = useGetPermissions(queryParams);
  const updatePermissionsMutation = useUpdatePermissions();

  const handleGetPermissions = (values: any) => {
    setQueryParams({
      resourceType: values.resourceType,
      resourceName: values.resourceName,
      principal: values.principal,
    });
  };

  const handleGrantPermission = async (values: PermissionForm) => {
    try {
      await updatePermissionsMutation.mutateAsync({
        resourceType: values.resourceType,
        resourceName: values.resourceName,
        changes: [{
          principal: values.principal,
          add: [values.permission],
          remove: [],
        }],
      });
      message.success('Permission granted successfully');
      grantForm.resetFields();
      if (queryParams.resourceType && queryParams.resourceName) {
        refetch();
      }
    } catch (error) {
      message.error('Failed to grant permission');
    }
  };

  const handleRevokePermission = async (principal: string, privilege: Privilege) => {
    if (!queryParams.resourceType || !queryParams.resourceName) {
      message.error('Please query permissions first');
      return;
    }

    try {
      await updatePermissionsMutation.mutateAsync({
        resourceType: queryParams.resourceType,
        resourceName: queryParams.resourceName,
        changes: [{
          principal,
          add: [],
          remove: [privilege],
        }],
      });
      message.success('Permission revoked successfully');
      refetch();
    } catch (error) {
      message.error('Failed to revoke permission');
    }
  };

  const columns = [
    {
      title: 'Principal (Email/UPN)',
      dataIndex: 'principal',
      key: 'principal',
    },
    {
      title: 'Privileges',
      dataIndex: 'privileges',
      key: 'privileges',
      render: (privileges: Privilege[], record: any) => (
        <Space wrap>
          {privileges.map((privilege) => (
            <Button
              key={privilege}
              size="small"
              danger
              icon={<DeleteOutlined />}
              onClick={() => handleRevokePermission(record.principal, privilege)}
            >
              {privilege}
            </Button>
          ))}
        </Space>
      ),
    },
  ];

  return (
    <div>
      {/* Get Permissions Section */}
      <Card title="Query Permissions" style={{ marginBottom: 16 }}>
        <Form form={queryForm} onFinish={handleGetPermissions} layout="inline">
          <Form.Item
            name="resourceType"
            label="Resource Type"
            rules={[{ required: true, message: 'Please select resource type' }]}
          >
            <Select placeholder="Select resource type" options={resourceTypes} />
          </Form.Item>
          <Form.Item
            name="resourceName"
            label="Resource Name"
            rules={[{ required: true, message: 'Please enter resource name' }]}
          >
            <Input placeholder="e.g., main.default.my_table" />
          </Form.Item>
          <Form.Item name="principal" label="Principal (Optional)">
            <Input placeholder="user@domain.com" />
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit" icon={<SearchOutlined />}>
              Get Permissions
            </Button>
          </Form.Item>
        </Form>
      </Card>

      {/* Grant Permission Section */}
      <Card title="Grant Permission" style={{ marginBottom: 16 }}>
        <Form form={grantForm} onFinish={handleGrantPermission} layout="inline">
          <Form.Item
            name="resourceType"
            label="Resource Type"
            rules={[{ required: true, message: 'Please select resource type' }]}
          >
            <Select placeholder="Select resource type" options={resourceTypes} />
          </Form.Item>
          <Form.Item
            name="resourceName"
            label="Resource Name"
            rules={[{ required: true, message: 'Please enter resource name' }]}
          >
            <Input placeholder="e.g., main.default.my_table" />
          </Form.Item>
          <Form.Item
            name="principal"
            label="Principal (Email/UPN)"
            rules={[{ required: true, message: 'Please enter principal' }]}
          >
            <Input placeholder="user@domain.com" />
          </Form.Item>
          <Form.Item
            name="permission"
            label="Permission"
            rules={[{ required: true, message: 'Please select permission' }]}
          >
            <Select placeholder="Select permission" options={privileges} />
          </Form.Item>
          <Form.Item>
            <Button
              type="primary"
              htmlType="submit"
              icon={<PlusOutlined />}
              loading={updatePermissionsMutation.isPending}
            >
              Grant
            </Button>
          </Form.Item>
        </Form>
      </Card>

      {/* Permissions Results */}
      {permissions && (
        <Card title="Current Permissions">
          <Table
            columns={columns}
            dataSource={permissions.privilege_assignments}
            loading={isLoading}
            rowKey="principal"
            pagination={false}
          />
        </Card>
      )}
    </div>
  );
}
