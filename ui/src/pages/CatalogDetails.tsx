import React from 'react';
import { Link, useParams } from 'react-router-dom';
import DetailsLayout from '../components/layouts/DetailsLayout';
import { Flex, Typography } from 'antd';
import { useGetCatalog } from '../hooks/catalog';
import SchemasList from '../components/schemas/SchemasList';
import DescriptionBox from '../components/DescriptionBox';

export default function CatalogDetails() {
  const { catalog } = useParams();
  if (!catalog) throw new Error('Catalog name is required');

  const { data } = useGetCatalog({ catalog });

  if (!data) return null;

  return (
    <DetailsLayout
      title={<Typography.Title level={3}>{catalog}</Typography.Title>}
      breadcrumbs={[
        { title: <Link to="/">Catalogs</Link>, key: '_home' },
        { title: catalog, key: catalog },
      ]}
    >
      <DetailsLayout.Content>
        <Flex vertical gap="middle">
          <DescriptionBox comment={data.comment} />
          <SchemasList catalog={catalog} />
        </Flex>
      </DetailsLayout.Content>
      <DetailsLayout.Aside>
        <Typography.Title level={5}>Catalog Details</Typography.Title>
        <Typography.Text>Details will go here...</Typography.Text>
      </DetailsLayout.Aside>
    </DetailsLayout>
  );
}
