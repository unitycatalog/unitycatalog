import { Typography } from 'antd';
import MetadataList, { MetadataListType } from '../MetadataList';
import { formatTimestamp } from '../../utils/formatTimestamp';
import { ModelInterface, useGetModel } from '../../hooks/models';

const MODEL_METADATA: MetadataListType<ModelInterface> = [
  {
    key: 'created_at',
    label: 'Created at',
    dataIndex: 'created_at',
    render: (value) => (
      <Typography.Text>{formatTimestamp(value)}</Typography.Text>
    ),
  },
  {
    key: 'created_by',
    label: 'Created by',
    dataIndex: 'created_by',
    render: (value) => <Typography.Text>{value}</Typography.Text>,
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
    key: 'updated_by',
    label: 'Updated by',
    dataIndex: 'updated_by',
    render: (value) => <Typography.Text>{value}</Typography.Text>,
  },
];

interface ModelSidebarProps {
  catalog: string;
  schema: string;
  model: string;
}
export default function ModelSidebar({
  catalog,
  schema,
  model,
}: ModelSidebarProps) {
  const { data } = useGetModel({
    full_name: [catalog, schema, model].join('.'),
  });

  if (!data) return null;

  return (
    <MetadataList title="Model details" metadata={MODEL_METADATA} data={data} />
  );
}
