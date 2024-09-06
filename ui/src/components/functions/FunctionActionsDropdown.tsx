import { DeleteOutlined, MoreOutlined } from '@ant-design/icons';
import { Button, Dropdown, MenuProps } from 'antd';
import { useMemo, useState } from 'react';
import { DeleteFunctionModal } from '../modals/DeleteFunctionModal';

interface FunctionActionDropdownProps {
  catalog: string;
  schema: string;
  ucFunction: string;
}

enum FunctionActionsEnum {
  Delete,
}

export default function FunctionActionsDropdown({
  catalog,
  schema,
  ucFunction,
}: FunctionActionDropdownProps) {
  const [dropdownVisible, setDropdownVisible] = useState<boolean>(false);
  const [action, setAction] = useState<FunctionActionsEnum | null>(null);

  const menuItems = useMemo(
    (): MenuProps['items'] => [
      {
        key: 'deleteFunction',
        label: 'Delete Function',
        onClick: () => setAction(FunctionActionsEnum.Delete),
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
      <DeleteFunctionModal
        open={action === FunctionActionsEnum.Delete}
        closeModal={() => setAction(null)}
        catalog={catalog}
        schema={schema}
        ucFunction={ucFunction}
      />
    </>
  );
}
