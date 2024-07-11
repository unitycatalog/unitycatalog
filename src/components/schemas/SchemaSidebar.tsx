import { Typography } from 'antd';
import { formatTimestamp } from '../../utils/formatTimestamp';
import MetadataList, { MetadataListType } from '../MetadataList';
import { SchemaInterface, useGetSchema } from '../../hooks/schemas';

interface SchemaSidebarProps {
  catalog: string;
  schema: string;
}

const SCHEMA_METADATA: MetadataListType<SchemaInterface> = [
  {
    key: 'created_at',
    dataIndex: 'created_at',
    render: (value) => (
      <Typography.Text>{formatTimestamp(value)}</Typography.Text>
    ),
  },
  {
    key: 'updated_at',
    dataIndex: 'updated_at',
    render: (value) => (
      <Typography.Text>{formatTimestamp(value)}</Typography.Text>
    ),
  },
];

export default function SchemaSidebar({ catalog, schema }: SchemaSidebarProps) {
  const { data } = useGetSchema({ catalog, schema });

  if (!data) return null;

  return (
    <MetadataList
      data={data}
      metadata={SCHEMA_METADATA}
      title="Schema details"
    />
  );
}
