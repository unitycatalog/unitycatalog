import { Typography } from 'antd';
import { formatTimestamp } from '../../utils/formatTimestamp';
import MetadataList, { MetadataListType } from '../MetadataList';
import { TableInterface, useGetTable } from '../../hooks/tables';

interface TableSidebarProps {
  catalog: string;
  schema: string;
  table: string;
}

const TABLE_METADATA: MetadataListType<Omit<TableInterface, 'columns'>> = [
  {
    key: 'created_at',
    label: 'Created at',
    dataIndex: 'created_at',
    render: (value) => (
      <Typography.Text>{formatTimestamp(value)}</Typography.Text>
    ),
  },
  {
    key: 'updated_at',
    label: 'Updated at',
    dataIndex: 'updated_at',
    render: (value) => (
      <Typography.Text>{formatTimestamp(value)}</Typography.Text>
    ),
  },
  {
    key: 'data_source_format',
    label: 'Data source format',
    dataIndex: 'data_source_format',
    render: (value) => <Typography.Text code>{value}</Typography.Text>,
  },
];

export default function TableSidebar({
  catalog,
  schema,
  table,
}: TableSidebarProps) {
  const { data } = useGetTable({
    full_name: [catalog, schema, table].join('.'),
  });

  if (!data) return null;

  const { columns, ...metadata } = data;

  return (
    <MetadataList
      data={metadata}
      metadata={TABLE_METADATA}
      title="Table details"
    />
  );
}
