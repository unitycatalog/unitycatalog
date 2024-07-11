import React from 'react';
import { Link, useParams } from 'react-router-dom';
import { useGetTable } from '../hooks/tables';
import DetailsLayout from '../components/layouts/DetailsLayout';
import { Flex, Typography } from 'antd';
import DescriptionBox from '../components/DescriptionBox';
import ColumnsList from '../components/tables/ColumnsList';
import TableSidebar from '../components/tables/TablesSidebar';
import { TableOutlined } from '@ant-design/icons';

export default function TableDetails() {
  const { catalog, schema, table } = useParams();
  if (!catalog) throw new Error('Catalog name is required');
  if (!schema) throw new Error('Schema name is required');
  if (!table) throw new Error('Table name is required');

  const { data } = useGetTable({ catalog, schema, table });

  if (!data) return null;

  const tableFullName = [catalog, schema, table].join('.');
  return (
    <DetailsLayout
      title={
        <Typography.Title level={3}>
          <TableOutlined /> {tableFullName}
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
        { title: table, key: '_table' },
      ]}
    >
      <DetailsLayout.Content>
        <Flex vertical gap="middle">
          <DescriptionBox comment={data.comment} />
          <ColumnsList catalog={catalog} schema={schema} table={table} />
        </Flex>
      </DetailsLayout.Content>
      <DetailsLayout.Aside>
        <TableSidebar catalog={catalog} schema={schema} table={table} />
      </DetailsLayout.Aside>
    </DetailsLayout>
  );
}
