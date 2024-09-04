import { DeleteOutlined, MoreOutlined } from '@ant-design/icons';
import { Button, Dropdown, MenuProps } from 'antd';
import { useMemo, useState } from 'react';
import { DeleteVolumeModal } from '../modals/DeleteVolumeModal';

interface VolumeActionDropdownProps {
  catalog: string;
  schema: string;
  volume: string;
}

enum VolumeActionsEnum {
  Delete,
}

export default function VolumeActionsDropdown({
  catalog,
  schema,
  volume,
}: VolumeActionDropdownProps) {
  const [dropdownVisible, setDropdownVisible] = useState<boolean>(false);
  const [action, setAction] = useState<VolumeActionsEnum | null>(null);

  const menuItems = useMemo(
    (): MenuProps['items'] => [
      {
        key: 'deleteVolume',
        label: 'Delete Volume',
        onClick: () => setAction(VolumeActionsEnum.Delete),
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
      <DeleteVolumeModal
        open={action === VolumeActionsEnum.Delete}
        closeModal={() => setAction(null)}
        catalog={catalog}
        schema={schema}
        volume={volume}
      />
    </>
  );
}
