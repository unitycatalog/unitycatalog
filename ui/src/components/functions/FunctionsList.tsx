import { Typography } from 'antd';
import ListLayout from '../layouts/ListLayout';
import { formatTimestamp } from '../../utils/formatTimestamp';
import { useNavigate } from 'react-router-dom';
import { ReactNode } from 'react';
import { useListFunctions } from '../../hooks/functions';

interface FunctionsListProps {
  catalog: string;
  schema: string;
  filters?: ReactNode;
}

export default function FunctionsList({
  catalog,
  schema,
  filters,
}: FunctionsListProps) {
  const { data, isLoading } = useListFunctions({ catalog, schema });
  const navigate = useNavigate();

  return (
    <ListLayout
      loading={isLoading}
      title={<Typography.Title level={4}>Functions</Typography.Title>}
      data={data?.functions}
      filters={filters}
      onRowClick={(record) =>
        navigate(
          `/functions/${record.catalog_name}/${record.schema_name}/${record.name}`
        )
      }
      columns={[
        {
          title: 'Name',
          dataIndex: 'name',
          key: 'name',
          width: '60%',
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
