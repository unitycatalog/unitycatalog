import { DeleteOutlined, MoreOutlined } from '@ant-design/icons';
import { Button, Dropdown, MenuProps } from 'antd';
import { useMemo, useState } from 'react';
import { DeleteCatalogModal } from '../modals/DeleteCatalogModal';

interface CatalogActionDropdownProps {
  catalog: string;
}

enum CatalogActionsEnum {
  Delete,
}

export default function CatalogActionsDropdown({
  catalog,
}: CatalogActionDropdownProps) {
  const [dropdownVisible, setDropdownVisible] = useState<boolean>(false);
  const [action, setAction] = useState<CatalogActionsEnum | null>(null);

  const menuItems = useMemo(
    (): MenuProps['items'] => [
      {
        key: 'deleteCatalog',
        label: 'Delete Catalog',
        onClick: () => setAction(CatalogActionsEnum.Delete),
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
      <DeleteCatalogModal
        open={action === CatalogActionsEnum.Delete}
        closeModal={() => setAction(null)}
        catalog={catalog}
      />
    </>
  );
}
