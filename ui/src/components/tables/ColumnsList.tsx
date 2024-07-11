import { Typography } from 'antd';
import ListLayout from '../layouts/ListLayout';
import { formatTimestamp } from '../../utils/formatTimestamp';
import { useGetTable } from '../../hooks/tables';

interface ColumnsListProps {
  catalog: string;
  schema: string;
  table: string;
}

export default function ColumnsList({
  catalog,
  schema,
  table,
}: ColumnsListProps) {
  const { data, isLoading } = useGetTable({ catalog, schema, table });

  return (
    <ListLayout
      loading={isLoading}
      title={<Typography.Title level={4}>Columns</Typography.Title>}
      data={data?.columns}
      columns={[
        {
          title: 'Name',
          dataIndex: 'name',
          key: 'name',
          width: '60%',
        },
        {
          title: 'Type',
          dataIndex: 'type_name',
          key: 'type',
          width: '40%',
        },
      ]}
    />
  );
}
