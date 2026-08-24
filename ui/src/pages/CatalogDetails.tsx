import React, { useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import DetailsLayout from '../components/layouts/DetailsLayout';
import { Flex, Typography } from 'antd';
import { useGetCatalog, useUpdateCatalog } from '../hooks/catalog';
import SchemasList from '../components/schemas/SchemasList';
import DescriptionBox from '../components/DescriptionBox';
import CatalogSidebar from '../components/catalogs/CatalogSidebar';
import { ProductOutlined } from '@ant-design/icons';
import CreateSchemaAction from '../components/schemas/CreateSchemaAction';
import CatalogActionDropdown from '../components/catalogs/CatalogActionDropdown';
import { EditCatalogDescriptionModal } from '../components/modals/EditCatalogDescriptionModal';
import { useNotification } from '../utils/NotificationContext';

export default function CatalogDetails() {
  const { catalog } = useParams();
  if (!catalog) throw new Error('Catalog name is required');

  const { data } = useGetCatalog({ name: catalog });
  const [open, setOpen] = useState<boolean>(false);
  const { setNotification } = useNotification();
  const mutation = useUpdateCatalog({ name: catalog });

  if (!data) return null;

  return (
    <>
      <DetailsLayout
        title={
          <Flex justify="space-between" align="flex-start" gap="middle">
            <Typography.Title level={3}>
              <ProductOutlined /> {catalog}
            </Typography.Title>
            <Flex gap="middle">
              <CatalogActionDropdown catalog={catalog} />
              <CreateSchemaAction catalog={catalog} />
            </Flex>
          </Flex>
        }
        breadcrumbs={[
          { title: <Link to="/">Catalogs</Link>, key: '_home' },
          { title: catalog, key: '_catalog' },
        ]}
      >
        <DetailsLayout.Content>
          <Flex vertical gap="middle">
            <DescriptionBox
              comment={data.comment ?? ''}
              onEdit={() => setOpen(true)}
            />
            <SchemasList catalog={catalog} />
          </Flex>
        </DetailsLayout.Content>
        <DetailsLayout.Aside>
          <CatalogSidebar catalog={catalog} />
        </DetailsLayout.Aside>
      </DetailsLayout>
      {/* NOTE: Due to inconsistencies in the API specification, the following workaround is necessary. 
          This inconsistency can still be addressed at the interface level. */}
      <EditCatalogDescriptionModal
        open={open}
        catalog={{ comment: data.comment }}
        closeModal={() => setOpen(false)}
        onSubmit={(values) =>
          mutation.mutate(values, {
            onError: (error: Error) => {
              setNotification(error.message, 'error');
            },
            onSuccess: (catalog) => {
              setNotification(
                `${catalog.name} catalog successfully updated`,
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
