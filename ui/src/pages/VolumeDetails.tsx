import React, { useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import DetailsLayout from '../components/layouts/DetailsLayout';
import { Flex, Typography } from 'antd';
import DescriptionBox from '../components/DescriptionBox';
import { useGetVolume, useUpdateVolume } from '../hooks/volumes';
import VolumeSidebar from '../components/volumes/VolumeSidebar';
import { FolderOutlined } from '@ant-design/icons';
import VolumeActionsDropdown from '../components/volumes/VolumeActionsDropdown';
import { EditVolumeDescriptionModal } from '../components/modals/EditVolumeDescriptionModal';
import { useNotification } from '../utils/NotificationContext';

export default function VolumeDetails() {
  const { catalog, schema, volume } = useParams();
  if (!catalog) throw new Error('Catalog name is required');
  if (!schema) throw new Error('Schema name is required');
  if (!volume) throw new Error('Volume name is required');

  const { data } = useGetVolume({ name: [catalog, schema, volume].join('.') });
  const [open, setOpen] = useState<boolean>(false);
  const { setNotification } = useNotification();
  const mutation = useUpdateVolume({
    name: [catalog, schema, volume].join('.'),
  });

  if (!data) return null;

  const volumeFullName = [catalog, schema, volume].join('.');
  return (
    <>
      <DetailsLayout
        title={
          <Flex justify="space-between" align="flex-start" gap="middle">
            <Typography.Title level={3}>
              <FolderOutlined /> {volumeFullName}
            </Typography.Title>
            <VolumeActionsDropdown
              catalog={catalog}
              schema={schema}
              volume={volume}
            />
          </Flex>
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
            <DescriptionBox
              comment={data.comment || ''}
              onEdit={() => setOpen(true)}
            />
          </Flex>
        </DetailsLayout.Content>
        <DetailsLayout.Aside>
          <VolumeSidebar catalog={catalog} schema={schema} volume={volume} />
        </DetailsLayout.Aside>
      </DetailsLayout>
      <EditVolumeDescriptionModal
        open={open}
        volume={data}
        closeModal={() => setOpen(false)}
        onSubmit={(values) =>
          mutation.mutate(values, {
            onError: (error: Error) => {
              setNotification(error.message, 'error');
            },
            onSuccess: (volume) => {
              setNotification(
                `${volume.name} volume successfully updated`,
                'success',
              );
              setOpen(false);
            },
          })
        }
        loading={mutation.isPending}
      />
    </>
  );
}
