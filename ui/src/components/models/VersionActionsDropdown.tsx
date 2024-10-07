import { DeleteOutlined, MoreOutlined } from '@ant-design/icons';
import { Button, Dropdown, MenuProps } from 'antd';
import { useMemo, useState } from 'react';
import { DeleteModelVersionModal } from '../modals/DeleteModelVersionModal';

interface VersionActionDropdownProps {
  catalog: string;
  schema: string;
  model: string;
  version: number;
}

enum VersionActionsEnum {
  Delete,
}

export default function VersionActionsDropdown({
  catalog,
  schema,
  model,
  version,
}: VersionActionDropdownProps) {
  const [dropdownVisible, setDropdownVisible] = useState<boolean>(false);
  const [action, setAction] = useState<VersionActionsEnum | null>(null);

  const menuItems = useMemo(
    (): MenuProps['items'] => [
      {
        key: 'deleteFunction',
        label: 'Delete Model Version',
        onClick: () => setAction(VersionActionsEnum.Delete),
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
      <DeleteModelVersionModal
        open={action === VersionActionsEnum.Delete}
        closeModal={() => setAction(null)}
        catalog={catalog}
        schema={schema}
        model={model}
        version={version}
      />
    </>
  );
}
