import { DeleteOutlined, MoreOutlined } from '@ant-design/icons';
import { Button, Dropdown, MenuProps } from 'antd';
import { useMemo, useState } from 'react';
import { DeleteModelModal } from '../modals/DeleteModelModal';

interface ModelActionDropdownProps {
  catalog: string;
  schema: string;
  model: string;
  hasExistingVersions: boolean;
}

enum ModelActionsEnum {
  Delete,
}

export default function ModelActionsDropdown({
  catalog,
  schema,
  model,
  hasExistingVersions,
}: ModelActionDropdownProps) {
  const [dropdownVisible, setDropdownVisible] = useState<boolean>(false);
  const [action, setAction] = useState<ModelActionsEnum | null>(null);

  const menuItems = useMemo(
    (): MenuProps['items'] => [
      {
        key: 'deleteModel',
        label: 'Delete Model',
        onClick: () => setAction(ModelActionsEnum.Delete),
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
      <DeleteModelModal
        open={action === ModelActionsEnum.Delete}
        closeModal={() => setAction(null)}
        catalog={catalog}
        schema={schema}
        model={model}
        hasExistingVersions={hasExistingVersions}
      />
    </>
  );
}
