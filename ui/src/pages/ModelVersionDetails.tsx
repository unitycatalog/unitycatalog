import React, { useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import DetailsLayout from '../components/layouts/DetailsLayout';
import DescriptionBox from '../components/DescriptionBox';
import { Flex, Table, TableProps, Typography } from 'antd';
import { DeploymentUnitOutlined } from '@ant-design/icons';
import { useGetModelVersion, useUpdateModelVersion } from '../hooks/models';
import ModelSidebar from '../components/models/ModelSidebar';
import { formatTimestamp } from '../utils/formatTimestamp';
import VersionActionsDropdown from '../components/models/VersionActionsDropdown';
import { useNotification } from '../utils/NotificationContext';
import { EditVersionDescriptionModal } from '../components/modals/EditVersionDescriptionModal';

interface DataType {
  versionKey: string;
  versionValue: string;
}

export default function ModelVersionDetails() {
  const { catalog, schema, model, version } = useParams();
  if (!catalog) throw new Error('Catalog name is required');
  if (!schema) throw new Error('Schema name is required');
  if (!model) throw new Error('Model name is required');
  if (!version) throw new Error('Version number is required');
  const { data, refetch } = useGetModelVersion({
    full_name: [catalog, schema, model].join('.'),
    version: Number(version),
  });
  const [open, setOpen] = useState<boolean>(false);
  const { setNotification } = useNotification();
  const mutation = useUpdateModelVersion({
    full_name: [catalog, schema, model].join('.'),
    version: Number(version),
  });

  if (!data) return null;

  const columns: TableProps<DataType>['columns'] = [
    {
      dataIndex: 'versionKey',
      rowScope: 'row',
    },
    {
      dataIndex: 'versionValue',
    },
  ];

  const versionData: DataType[] = [
    {
      versionKey: 'Created at',
      versionValue: data.created_at ? formatTimestamp(data.created_at) : '',
    },
    {
      versionKey: 'Created by',
      versionValue: data.created_by ?? '',
    },
    {
      versionKey: 'Updated at',
      versionValue: data.updated_at ? formatTimestamp(data.updated_at) : '',
    },
    {
      versionKey: 'Updated by',
      versionValue: data.updated_by ?? '',
    },
    {
      versionKey: 'Status',
      versionValue: data.status ?? '',
    },
    {
      versionKey: 'Training run',
      versionValue: data.run_id ?? '',
    },
  ];

  return (
    <>
      {' '}
      <DetailsLayout
        title={
          <Flex justify="space-between" align="flex-start" gap="middle">
            <Typography.Title level={3}>
              <DeploymentUnitOutlined /> {`${model} version ${version}`}
            </Typography.Title>
            <VersionActionsDropdown
              catalog={catalog}
              schema={schema}
              model={model}
              version={Number(version)}
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
          {
            title: (
              <Link to={`/models/${catalog}/${schema}/${model}`}>{model}</Link>
            ),
            key: '_model',
          },
          { title: `version ${version}`, key: '_version' },
        ]}
      >
        <DetailsLayout.Content>
          <Flex vertical gap={60}>
            <DescriptionBox
              comment={data.comment ?? ''}
              onEdit={() => setOpen(true)}
            />
            <div>
              <Typography.Title level={5} style={{ marginBottom: 16 }}>
                Version details
              </Typography.Title>
              <Table
                columns={columns}
                dataSource={versionData}
                pagination={false}
                showHeader={false}
                bordered
              />
            </div>
          </Flex>
        </DetailsLayout.Content>
        <DetailsLayout.Aside>
          <ModelSidebar catalog={catalog} schema={schema} model={model} />
        </DetailsLayout.Aside>
      </DetailsLayout>
      <EditVersionDescriptionModal
        open={open}
        version={data}
        closeModal={() => setOpen(false)}
        onSubmit={(values) =>
          mutation.mutate(values, {
            onError: (error: Error) => {
              setNotification(error.message, 'error');
            },
            onSuccess: (version) => {
              refetch().then(() => {
                setNotification(
                  `Model version ${version.version} successfully updated`,
                  'success',
                );
                setOpen(false);
              });
            },
          })
        }
        loading={mutation.isPending}
      />
    </>
  );
}
