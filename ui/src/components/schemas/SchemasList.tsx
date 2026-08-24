import { Typography } from 'antd';
import { useListSchemas } from '../../hooks/schemas';
import ListLayout from '../layouts/ListLayout';
import { formatTimestamp } from '../../utils/formatTimestamp';
import { useNavigate } from 'react-router-dom';
import { DatabaseOutlined } from '@ant-design/icons';

interface SchemasListProps {
  catalog: string;
}

export default function SchemasList({ catalog }: SchemasListProps) {
  const { data, isLoading } = useListSchemas({ catalog_name: catalog });
  const navigate = useNavigate();

  return (
    <ListLayout
      loading={isLoading}
      title={<Typography.Title level={4}>Schemas</Typography.Title>}
      data={data?.schemas}
      onRowClick={(record) => navigate(`/data/${catalog}/${record.name}`)}
      rowKey={(record) => `schema-${record.schema_id}`}
      columns={[
        {
          title: 'Name',
          dataIndex: 'name',
          key: 'name',
          width: '60%',
          render: (name) => (
            <>
              <DatabaseOutlined /> {name}
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
