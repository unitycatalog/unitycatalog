import React from 'react';
import { Link, useParams } from 'react-router-dom';
import DetailsLayout from '../components/layouts/DetailsLayout';
import { Flex, Radio, Typography } from 'antd';
import DescriptionBox from '../components/DescriptionBox';
import { useGetSchema } from '../hooks/schemas';
import SchemaSidebar from '../components/schemas/SchemaSidebar';
import TablesList from '../components/tables/TablesList';
import VolumesList from '../components/volumes/VolumesList';
import FunctionsList from '../components/functions/FunctionsList';
import { DatabaseOutlined } from '@ant-design/icons';
import CreateAssetsDropdown from '../components/schemas/CreateAssetsDropdown';

export default function SchemaDetails() {
  const { catalog, schema } = useParams();
  if (!catalog) throw new Error('Catalog name is required');
  if (!schema) throw new Error('Schema name is required');

  const { data } = useGetSchema({ catalog, schema });

  if (!data) return null;

  const schemaFullName = [catalog, schema].join('.');
  return (
    <DetailsLayout
      title={
        <Flex justify="space-between" align="flex-start" gap="middle">
          <Typography.Title level={3}>
            <DatabaseOutlined /> {schemaFullName}
          </Typography.Title>
          <CreateAssetsDropdown catalog={catalog} schema={schema} />
        </Flex>
      }
      breadcrumbs={[
        { title: <Link to="/">Catalogs</Link>, key: '_home' },
        {
          title: <Link to={`/data/${catalog}`}>{catalog}</Link>,
          key: '_catalog',
        },
        { title: schema, key: '_schema' },
      ]}
    >
      <DetailsLayout.Content>
        <Flex vertical gap="middle">
          <DescriptionBox comment={data.comment} />
          <SchemaDetailsTabs catalog={catalog} schema={schema} />
        </Flex>
      </DetailsLayout.Content>
      <DetailsLayout.Aside>
        <SchemaSidebar catalog={catalog} schema={schema} />
      </DetailsLayout.Aside>
    </DetailsLayout>
  );
}

enum SchemaTabs {
  Tables = 'Tables',
  Volumes = 'Volumes',
  Functions = 'Functions',
}

const SCHEMA_TABS_MAP = {
  [SchemaTabs.Tables]: TablesList,
  [SchemaTabs.Volumes]: VolumesList,
  [SchemaTabs.Functions]: FunctionsList,
};

interface SchemaDetailsTabsProps {
  catalog: string;
  schema: string;
}

function SchemaDetailsTabs({ catalog, schema }: SchemaDetailsTabsProps) {
  const [activeTab, setActiveTab] = React.useState<SchemaTabs>(
    SchemaTabs.Tables
  );
  const Component = SCHEMA_TABS_MAP[activeTab];

  return (
    <Component
      catalog={catalog}
      schema={schema}
      filters={
        <Radio.Group
          value={activeTab}
          onChange={(e) => setActiveTab(e.target.value)}
        >
          <Radio.Button value={SchemaTabs.Tables}>
            {SchemaTabs.Tables}
          </Radio.Button>
          <Radio.Button value={SchemaTabs.Volumes}>
            {SchemaTabs.Volumes}
          </Radio.Button>
          <Radio.Button value={SchemaTabs.Functions}>
            {SchemaTabs.Functions}
          </Radio.Button>
        </Radio.Group>
      }
    />
  );
}
