import React from 'react';
import { Link, useParams } from 'react-router-dom';
import { useGetTable } from '../hooks/tables';
import DetailsLayout from '../components/layouts/DetailsLayout';
import { Flex, Typography } from 'antd';
import ColumnsList from '../components/tables/ColumnsList';
import TableSidebar from '../components/tables/TablesSidebar';
import { TableOutlined } from '@ant-design/icons';
import TableActionsDropdown from '../components/tables/TableActionsDropdown';

export default function TableDetails() {
  const { catalog, schema, table } = useParams();
  if (!catalog) throw new Error('Catalog name is required');
  if (!schema) throw new Error('Schema name is required');
  if (!table) throw new Error('Table name is required');

  const { data } = useGetTable({
    full_name: [catalog, schema, table].join('.'),
  });

  if (!data) return null;

  const tableFullName = [catalog, schema, table].join('.');
  return (
    <DetailsLayout
      title={
        <Flex justify="space-between" align="flex-start" gap="middle">
          <Typography.Title level={3}>
            <TableOutlined /> {tableFullName}
          </Typography.Title>
          <TableActionsDropdown
            catalog={catalog}
            schema={schema}
            table={table}
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
        { title: table, key: '_table' },
      ]}
    >
      <DetailsLayout.Content>
        <Flex vertical gap="middle">
          <div>
            <Typography.Title level={5}>Description</Typography.Title>
            <Typography.Text type="secondary">
              {data.comment ?? ''}
            </Typography.Text>
          </div>
          <ColumnsList catalog={catalog} schema={schema} table={table} />
        </Flex>
      </DetailsLayout.Content>
      <DetailsLayout.Aside>
        <TableSidebar catalog={catalog} schema={schema} table={table} />
      </DetailsLayout.Aside>
    </DetailsLayout>
  );
}
