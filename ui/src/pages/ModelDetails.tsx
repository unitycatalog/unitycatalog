import React, { useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import DetailsLayout from '../components/layouts/DetailsLayout';
import { Flex, Typography } from 'antd';
import { DeploymentUnitOutlined } from '@ant-design/icons';
import {
  useGetModel,
  useListModelVersions,
  useUpdateModel,
} from '../hooks/models';
import ModelSidebar from '../components/models/ModelSidebar';
import { formatTimestamp } from '../utils/formatTimestamp';
import ListLayout from '../components/layouts/ListLayout';
import ModelVersionStatusDisplay from '../components/models/ModelVersionStatusDisplay';
import ModelActionsDropdown from '../components/models/ModelActionsDropdown';
import { useNotification } from '../utils/NotificationContext';
import { EditModelDescriptionModal } from '../components/modals/EditModelDescriptionModal';
import DescriptionBox from '../components/DescriptionBox';

export default function ModelDetails() {
  const { catalog, schema, model } = useParams();
  if (!catalog) throw new Error('Catalog name is required');
  if (!schema) throw new Error('Schema name is required');
  if (!model) throw new Error('Model name is required');
  const navigate = useNavigate();

  const { data, refetch } = useGetModel({
    full_name: [catalog, schema, model].join('.'),
  });
  const { data: versionData, isLoading } = useListModelVersions({
    full_name: [catalog, schema, model].join('.'),
  });
  const [open, setOpen] = useState<boolean>(false);
  const { setNotification } = useNotification();
  const mutation = useUpdateModel({
    full_name: [catalog, schema, model].join('.'),
  });
  if (!data) return null;

  return (
    <>
      <DetailsLayout
        title={
          <Flex justify="space-between" align="flex-start" gap="middle">
            <Typography.Title level={3}>
              <DeploymentUnitOutlined /> {model}
            </Typography.Title>
            <ModelActionsDropdown
              catalog={catalog}
              schema={schema}
              model={model}
              hasExistingVersions={Boolean(versionData?.model_versions?.length)}
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
          { title: model, key: '_model' },
        ]}
      >
        <DetailsLayout.Content>
          <Flex vertical gap={60}>
            <DescriptionBox
              comment={data.comment ?? ''}
              onEdit={() => setOpen(true)}
            />
            <ListLayout
              loading={isLoading}
              title={<Typography.Title level={5}>Versions</Typography.Title>}
              data={versionData?.model_versions || []}
              showSearch={false}
              onRowClick={(record) =>
                navigate(
                  `/models/${record.catalog_name}/${record.schema_name}/${record.model_name}/versions/${record.version}`,
                )
              }
              rowKey={(record) => `model-version-${record.id}`}
              columns={[
                {
                  title: 'Status',
                  dataIndex: 'status',
                  key: 'status',
                  width: '10%',
                  align: 'center',
                  render: (status) => (
                    <ModelVersionStatusDisplay status={status} />
                  ),
                },
                {
                  title: 'Name',
                  dataIndex: 'version',
                  key: 'version',
                  width: '60%',
                  render: (version) => <>{`Version ${version}`}</>,
                },
                {
                  title: 'Time registered',
                  dataIndex: 'created_at',
                  key: 'created_at',
                  width: '40%',
                  render: (value) => formatTimestamp(value),
                },
              ]}
            />
          </Flex>
        </DetailsLayout.Content>
        <DetailsLayout.Aside>
          <ModelSidebar catalog={catalog} schema={schema} model={model} />
        </DetailsLayout.Aside>
      </DetailsLayout>
      <EditModelDescriptionModal
        open={open}
        model={data}
        closeModal={() => setOpen(false)}
        onSubmit={(values) =>
          mutation.mutate(values, {
            onError: (error: Error) => {
              setNotification(error.message, 'error');
            },
            onSuccess: (version) => {
              refetch().then(() => {
                setNotification(
                  `${model} model successfully updated`,
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
