import React from 'react';
import { useListCatalogs } from '../hooks/catalog';
import ListLayout from '../components/layouts/ListLayout';
import { Flex, Typography } from 'antd';
import { formatTimestamp } from '../utils/formatTimestamp';
import { useNavigate } from 'react-router-dom';
import CreateCatalogAction from '../components/catalogs/CreateCatalogAction';

export default function CatalogsList() {
  const { data, isLoading } = useListCatalogs();
  const navigate = useNavigate();

  return (
    <ListLayout
      loading={isLoading}
      title={
        <Flex justify="space-between" align="flex-start" gap="middle">
          <Typography.Title level={2}>Catalogs</Typography.Title>
          <CreateCatalogAction />
        </Flex>
      }
      data={data?.catalogs}
      onRowClick={(record) => navigate(`/data/${record.name}`)}
      rowKey={(record) => `catalog-${record.id}`}
      columns={[
        { title: 'Name', dataIndex: 'name', key: 'name', width: '60%' },
        {
          title: 'Created At',
          dataIndex: 'created_at',
          key: 'created_at',
          width: '40%',
          render: (value) => formatTimestamp(value),
        },
      ]}
    />
  );
}
