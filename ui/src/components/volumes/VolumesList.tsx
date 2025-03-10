import { Typography } from 'antd';
import ListLayout from '../layouts/ListLayout';
import { formatTimestamp } from '../../utils/formatTimestamp';
import { useNavigate } from 'react-router-dom';
import { useListVolumes } from '../../hooks/volumes';
import { ReactNode } from 'react';
import { FolderOutlined } from '@ant-design/icons';

interface VolumesListProps {
  catalog: string;
  schema: string;
  filters?: ReactNode;
}

export default function VolumesList({
  catalog,
  schema,
  filters,
}: VolumesListProps) {
  const { data, isLoading } = useListVolumes({
    catalog_name: catalog,
    schema_name: schema,
  });
  const navigate = useNavigate();

  return (
    <ListLayout
      loading={isLoading}
      title={<Typography.Title level={4}>Volumes</Typography.Title>}
      data={data?.volumes}
      filters={filters}
      onRowClick={(record) =>
        navigate(
          `/volumes/${record.catalog_name}/${record.schema_name}/${record.name}`,
        )
      }
      rowKey={(record) => `volume-${record.volume_id}`}
      columns={[
        {
          title: 'Name',
          dataIndex: 'name',
          key: 'name',
          width: '60%',
          render: (name) => (
            <>
              <FolderOutlined /> {name}
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
