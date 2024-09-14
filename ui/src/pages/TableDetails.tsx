import React, { useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import DetailsLayout from '../components/layouts/DetailsLayout';
import { Flex, Typography } from 'antd';
import DescriptionBox from '../components/DescriptionBox';
import { useGetTable, useUpdateTable } from '../hooks/tables';
import ColumnsList from '../components/tables/ColumnsList';
import TableSidebar from '../components/tables/TablesSidebar';
import { TableOutlined } from '@ant-design/icons';
import TableActionsDropdown from '../components/tables/TableActionsDropdown';
import { EditTableDescriptionModal } from '../components/modals/EditTableDescriptionModal';
import { useNotification } from '../utils/NotificationContext';

export default function TableDetails() {
  const { catalog, schema, table } = useParams();
  if (!catalog) throw new Error('Catalog name is required');
  if (!schema) throw new Error('Schema name is required');
  if (!table) throw new Error('Table name is required');

  const { data } = useGetTable({ catalog, schema, table });
  const [open, setOpen] = useState<boolean>(false);
  const { setNotification } = useNotification();
  const mutation = useUpdateTable({ catalog, schema, table });

  if (!data) return null;

  const tableFullName = [catalog, schema, table].join('.');
  return (
    <>
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
            <DescriptionBox
              comment={data.comment}
              onEdit={() => setOpen(true)}
            />
            <ColumnsList catalog={catalog} schema={schema} table={table} />
          </Flex>
        </DetailsLayout.Content>
        <DetailsLayout.Aside>
          <TableSidebar catalog={catalog} schema={schema} table={table} />
        </DetailsLayout.Aside>
      </DetailsLayout>
      <EditTableDescriptionModal
        open={open}
        table={data}
        closeModal={() => setOpen(false)}
        onSubmit={(values) =>
          mutation.mutate(values, {
            onError: (error: Error) => {
              setNotification(error.message, 'error');
            },
            onSuccess: (table) => {
              setNotification(
                `${table.name} table successfully updated`,
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
