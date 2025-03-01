import { Typography } from 'antd';
import ListLayout from '../layouts/ListLayout';
import { formatTimestamp } from '../../utils/formatTimestamp';
import { useNavigate } from 'react-router-dom';
import { ReactNode } from 'react';
import { DeploymentUnitOutlined } from '@ant-design/icons';
import { useListModels } from '../../hooks/models';

interface ModelsListProps {
  catalog: string;
  schema: string;
  filters?: ReactNode;
}

export default function ModelsList({
  catalog,
  schema,
  filters,
}: ModelsListProps) {
  const { data, isLoading } = useListModels({
    catalog_name: catalog,
    schema_name: schema,
  });
  const navigate = useNavigate();

  return (
    <ListLayout
      loading={isLoading}
      title={<Typography.Title level={4}>Models</Typography.Title>}
      data={data?.registered_models}
      filters={filters}
      onRowClick={(record) =>
        navigate(
          `/models/${record.catalog_name}/${record.schema_name}/${record.name}`,
        )
      }
      rowKey={(record) => `model-${record.id}`}
      columns={[
        {
          title: 'Name',
          dataIndex: 'name',
          key: 'name',
          width: '60%',
          render: (name) => (
            <>
              <DeploymentUnitOutlined /> {name}
            </>
          ),
        },
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
