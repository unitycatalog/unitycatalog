import {
  DeleteOutlined,
  DeploymentUnitOutlined,
  MoreOutlined,
} from '@ant-design/icons';
import { Button, Dropdown, MenuProps } from 'antd';
import React, { useMemo, useState } from 'react';
import { DeleteSchemaModal } from '../modals/DeleteSchemaModal';
import CreateModelModal from '../modals/CreateModelModal';

interface SchemaActionDropdownProps {
  catalog: string;
  schema: string;
}

enum SchemaActionsEnum {
  Delete,
  CreateModel,
}

export default function SchemaActionsDropdown({
  catalog,
  schema,
}: SchemaActionDropdownProps) {
  const [dropdownVisible, setDropdownVisible] = useState<boolean>(false);
  const [action, setAction] = useState<SchemaActionsEnum | null>(null);

  const menuItems = useMemo(
    (): MenuProps['items'] => [
      {
        key: 'createModel',
        label: 'Create Registered Model',
        onClick: () => setAction(SchemaActionsEnum.CreateModel),
        icon: <DeploymentUnitOutlined />,
        danger: false,
      },
      {
        key: 'deleteSchema',
        label: 'Delete Schema',
        onClick: () => setAction(SchemaActionsEnum.Delete),
        icon: <DeleteOutlined />,
        danger: true,
      },
    ],
    [],
  );

  return (
    <>
      <Dropdown
        menu={{ items: menuItems }}
        trigger={['click']}
        onOpenChange={() => setDropdownVisible(!dropdownVisible)}
      >
        <Button
          type="text"
          icon={
            <MoreOutlined
              rotate={dropdownVisible ? 90 : 0}
              style={{ transition: 'transform 0.5s' }}
            />
          }
        />
      </Dropdown>
      <DeleteSchemaModal
        open={action === SchemaActionsEnum.Delete}
        closeModal={() => setAction(null)}
        catalog={catalog}
        schema={schema}
      />
      <CreateModelModal
        open={action === SchemaActionsEnum.CreateModel}
        closeModal={() => setAction(null)}
        catalog={catalog}
        schema={schema}
      />
    </>
  );
}
