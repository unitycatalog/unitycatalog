import { Typography } from 'antd';
import ListLayout from '../layouts/ListLayout';
import { formatTimestamp } from '../../utils/formatTimestamp';
import { useNavigate } from 'react-router-dom';
import { useListTables } from '../../hooks/tables';
import { ReactNode } from 'react';
import { TableOutlined } from '@ant-design/icons';

interface TablesListProps {
  catalog: string;
  schema: string;
  filters?: ReactNode;
}

export default function TablesList({
  catalog,
  schema,
  filters,
}: TablesListProps) {
  const { data, isLoading } = useListTables({
    catalog_name: catalog,
    schema_name: schema,
  });
  const navigate = useNavigate();

  return (
    <ListLayout
      loading={isLoading}
      filters={filters}
      title={<Typography.Title level={4}>Tables</Typography.Title>}
      data={data?.tables}
      onRowClick={(record) =>
        navigate(
          `/data/${record.catalog_name}/${record.schema_name}/${record.name}`,
        )
      }
      rowKey={(record) => `table-${record.table_id}`}
      columns={[
        {
          title: 'Name',
          dataIndex: 'name',
          key: 'name',
          width: '60%',
          render: (name) => (
            <>
              <TableOutlined /> {name}
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
