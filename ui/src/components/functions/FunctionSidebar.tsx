import { Typography } from 'antd';
import MetadataList, { MetadataListType } from '../MetadataList';
import { formatTimestamp } from '../../utils/formatTimestamp';
import { FunctionInterface, useGetFunction } from '../../hooks/functions';

const FUNCTION_METADATA: MetadataListType<Omit<FunctionInterface, 'columns'>> =
  [
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
      key: 'external_language',
      label: 'External language',
      dataIndex: 'external_language',
      render: (value) => <Typography.Text code>{value}</Typography.Text>,
    },
  ];

interface FunctionSidebarProps {
  catalog: string;
  schema: string;
  ucFunction: string;
}
export default function FunctionSidebar({
  catalog,
  schema,
  ucFunction,
}: FunctionSidebarProps) {
  const { data } = useGetFunction({
    name: [catalog, schema, ucFunction].join('.'),
  });

  if (!data) return null;

  return (
    <MetadataList
      title="Function details"
      metadata={FUNCTION_METADATA}
      data={data}
    />
  );
}
