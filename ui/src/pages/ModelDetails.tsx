import React from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import DetailsLayout from '../components/layouts/DetailsLayout';
import DescriptionBox from '../components/DescriptionBox';
import { Flex, Tooltip, Typography } from 'antd';
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  DeploymentUnitOutlined,
  MinusCircleOutlined,
} from '@ant-design/icons';
import {
  ModelVersionStatus,
  useGetModel,
  useGetModelVersions,
} from '../hooks/models';
import ModelSidebar from '../components/models/ModelSidebar';
import { formatTimestamp } from '../utils/formatTimestamp';
import ListLayout from '../components/layouts/ListLayout';

export default function ModelDetails() {
  const { catalog, schema, model } = useParams();
  if (!catalog) throw new Error('Catalog name is required');
  if (!schema) throw new Error('Schema name is required');
  if (!model) throw new Error('Model name is required');
  const navigate = useNavigate();
  const { data } = useGetModel({ catalog, schema, model });
  const { data: versionData, isLoading } = useGetModelVersions({
    catalog,
    schema,
    model,
  });
  console.log('versions', versionData);
  if (!data) return null;

  return (
    <DetailsLayout
      title={
        <Flex justify="space-between" align="flex-start" gap="middle">
          <Typography.Title level={3}>
            <DeploymentUnitOutlined /> {model}
          </Typography.Title>
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
          <div>
            <Typography.Title level={5}>Description</Typography.Title>
            <Typography.Text type="secondary">{data?.comment}</Typography.Text>
          </div>
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
            columns={[
              {
                title: 'Status',
                dataIndex: 'status',
                key: 'status',
                width: '10%',
                align: 'center',
                render: (status) => {
                  switch (status) {
                    case ModelVersionStatus.READY:
                      return (
                        <Tooltip title={`READY`}>
                          <CheckCircleOutlined
                            style={{ fontSize: '18px', color: 'green' }}
                          />
                        </Tooltip>
                      );
                    case ModelVersionStatus.PENDING_REGISTRATION:
                      return (
                        <Tooltip title={`PENDING REGISTRATION`}>
                          <MinusCircleOutlined
                            style={{ fontSize: '18px', color: 'gray' }}
                          />
                        </Tooltip>
                      );
                    case ModelVersionStatus.FAILED_REGISTRATION:
                      return (
                        <Tooltip title={`FAILED REGISTRATION`}>
                          <CloseCircleOutlined
                            style={{ fontSize: '18px', color: 'red' }}
                          />
                        </Tooltip>
                      );
                    default:
                      return (
                        <Typography.Text type="secondary">
                          UNKNOWN
                        </Typography.Text>
                      );
                  }
                },
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
  );
}
