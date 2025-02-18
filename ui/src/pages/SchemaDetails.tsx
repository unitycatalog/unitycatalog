import React, { useEffect, useState } from 'react';
import { Link, useLocation, useParams } from 'react-router-dom';
import DetailsLayout from '../components/layouts/DetailsLayout';
import { Flex, Radio, Typography } from 'antd';
import DescriptionBox from '../components/DescriptionBox';
import { useGetSchema, useUpdateSchema } from '../hooks/schemas';
import SchemaSidebar from '../components/schemas/SchemaSidebar';
import TablesList from '../components/tables/TablesList';
import VolumesList from '../components/volumes/VolumesList';
import FunctionsList from '../components/functions/FunctionsList';
import { DatabaseOutlined } from '@ant-design/icons';
// import CreateAssetsDropdown from '../components/schemas/CreateAssetsDropdown';
import SchemaActionsDropdown from '../components/schemas/SchemaActionDropdown';
import { EditSchemaDescriptionModal } from '../components/modals/EditSchemaDescriptionModal';
import { useNotification } from '../utils/NotificationContext';
import ModelsList from '../components/models/ModelsList';

export enum SchemaTabs {
  Tables = 'Tables',
  Volumes = 'Volumes',
  Functions = 'Functions',
  Models = 'Models',
}

const SCHEMA_TABS_MAP = {
  [SchemaTabs.Tables]: TablesList,
  [SchemaTabs.Volumes]: VolumesList,
  [SchemaTabs.Functions]: FunctionsList,
  [SchemaTabs.Models]: ModelsList,
};

export default function SchemaDetails() {
  const { catalog, schema } = useParams();
  const location = useLocation();
  const [activeTab, setActiveTab] = useState(
    location?.state ? location.state.tab : SchemaTabs.Tables,
  );

  useEffect(() => {
    if (location?.state?.tab && location?.state?.tab !== activeTab) {
      setActiveTab(location.state.tab);
    }
  }, [location, activeTab]);

  if (!catalog) throw new Error('Catalog name is required');
  if (!schema) throw new Error('Schema name is required');

  const { data } = useGetSchema({ full_name: [catalog, schema].join('.') });
  const [open, setOpen] = useState<boolean>(false);
  const { setNotification } = useNotification();
  const mutation = useUpdateSchema({ full_name: [catalog, schema].join('.') });

  if (!data) return null;

  const schemaFullName = [catalog, schema].join('.');

  return (
    <>
      <DetailsLayout
        title={
          <Flex justify="space-between" align="flex-start" gap="middle">
            <Typography.Title level={3}>
              <DatabaseOutlined /> {schemaFullName}
            </Typography.Title>
            <Flex gap="middle">
              <SchemaActionsDropdown catalog={catalog} schema={schema} />
              {/*<CreateAssetsDropdown catalog={catalog} schema={schema} />*/}
            </Flex>
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
            <DescriptionBox
              comment={data.comment ?? ''}
              onEdit={() => setOpen(true)}
            />
            <SchemaDetailsTabs
              catalog={catalog}
              schema={schema}
              tab={activeTab}
            />
          </Flex>
        </DetailsLayout.Content>
        <DetailsLayout.Aside>
          <SchemaSidebar catalog={catalog} schema={schema} />
        </DetailsLayout.Aside>
      </DetailsLayout>
      <EditSchemaDescriptionModal
        open={open}
        schema={data}
        closeModal={() => setOpen(false)}
        onSubmit={(values) =>
          mutation.mutate(values, {
            onError: (error: Error) => {
              setNotification(error.message, 'error');
            },
            onSuccess: (schema) => {
              setNotification(
                `${schema.name} schema successfully updated`,
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

interface SchemaDetailsTabsProps {
  catalog: string;
  schema: string;
  tab: SchemaTabs;
}

function SchemaDetailsTabs({ catalog, schema, tab }: SchemaDetailsTabsProps) {
  const [activeTab, setActiveTab] = React.useState<SchemaTabs>(tab);

  useEffect(() => {
    setActiveTab(tab);
  }, [tab]);

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
          <Radio.Button value={SchemaTabs.Models}>
            {SchemaTabs.Models}
          </Radio.Button>
        </Radio.Group>
      }
    />
  );
}
