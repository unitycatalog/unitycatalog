import React from 'react';
import { Link, useParams } from 'react-router-dom';
import DetailsLayout from '../components/layouts/DetailsLayout';
import { Flex, Typography } from 'antd';
import DescriptionBox from '../components/DescriptionBox';
import { useGetVolume } from '../hooks/volumes';
import VolumeSidebar from '../components/volumes/VolumeSidebar';
import { FolderOutlined } from '@ant-design/icons';

export default function VolumeDetails() {
  const { catalog, schema, volume } = useParams();
  if (!catalog) throw new Error('Catalog name is required');
  if (!schema) throw new Error('Schema name is required');
  if (!volume) throw new Error('Table name is required');

  const { data } = useGetVolume({ catalog, schema, volume });

  if (!data) return null;

  const volumeFullName = [catalog, schema, volume].join('.');
  return (
    <DetailsLayout
      title={
        <Typography.Title level={3}>
          <FolderOutlined /> {volumeFullName}
        </Typography.Title>
      }
      breadcrumbs={[
        { title: <Link to="/">Catalogs</Link>, key: '_home' },
        {
          title: <Link to={`/data/${catalog}`}>{catalog}</Link>,
          key: '_catalog',
        },
        {
          title: <Link to={`/data/${catalog}/${schema}`}>{schema}</Link>,
          key: '_schema',
        },
        { title: volume, key: '_volume' },
      ]}
    >
      <DetailsLayout.Content>
        <Flex vertical gap="middle">
          <DescriptionBox comment={data.comment} />
        </Flex>
      </DetailsLayout.Content>
      <DetailsLayout.Aside>
        <VolumeSidebar catalog={catalog} schema={schema} volume={volume} />
      </DetailsLayout.Aside>
    </DetailsLayout>
  );
}
