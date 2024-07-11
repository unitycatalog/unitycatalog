import React from 'react';
import { useListCatalogs } from '../hooks/catalog';
import ListLayout from '../components/layouts/ListLayout';
import { Typography } from 'antd';
import { formatTimestamp } from '../utils/formatTimestamp';
import { useNavigate } from 'react-router-dom';

export default function CatalogsList() {
  const { data, isLoading } = useListCatalogs();
  const navigate = useNavigate();

  return (
    <ListLayout
      loading={isLoading}
      title={<Typography.Title level={2}>Catalogs</Typography.Title>}
      data={data?.catalogs}
      onRowClick={(record) => navigate(`/data/${record.name}`)}
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
