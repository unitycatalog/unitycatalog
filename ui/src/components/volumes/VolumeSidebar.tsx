import { Typography } from 'antd';
import { formatTimestamp } from '../../utils/formatTimestamp';
import MetadataList, { MetadataListType } from '../MetadataList';
import { useGetVolume, VolumeInterface } from '../../hooks/volumes';

interface VolumeSidebarProps {
  catalog: string;
  schema: string;
  volume: string;
}

const VOLUME_METADATA: MetadataListType<VolumeInterface> = [
  {
    key: 'created_at',
    dataIndex: 'created_at',
    label: 'Created at',
    render: (value) => (
      <Typography.Text>{formatTimestamp(value)}</Typography.Text>
    ),
  },
  {
    key: 'updated_at',
    dataIndex: 'updated_at',
    label: 'Updated at',
    render: (value) => (
      <Typography.Text>{formatTimestamp(value)}</Typography.Text>
    ),
  },
  {
    key: 'volume_type',
    label: 'Volume type',
    dataIndex: 'volume_type',
    render: (value) => <Typography.Text code>{value}</Typography.Text>,
  },
];

export default function VolumeSidebar({
  catalog,
  schema,
  volume,
}: VolumeSidebarProps) {
  const { data } = useGetVolume({ name: [catalog, schema, volume].join('.') });

  if (!data) return null;

  return (
    <MetadataList
      data={data}
      metadata={VOLUME_METADATA}
      title="Volume details"
    />
  );
}
