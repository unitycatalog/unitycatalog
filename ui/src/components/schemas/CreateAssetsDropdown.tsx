import { DownOutlined } from '@ant-design/icons';
import { Button, Dropdown, MenuProps } from 'antd';
import { useMemo } from 'react';

interface CreateAssetsDropdownProps {
  catalog: string;
  schema: string;
}

enum CreateItemsEnum {
  Table,
  Volume,
  Function,
}

export default function CreateAssetsDropdown({
  catalog,
  schema,
}: CreateAssetsDropdownProps) {
  // const [openModal, setOpenModal] = useState<CreateItemsEnum | null>(null);

  const menuItems = useMemo(
    (): MenuProps['items'] => [
      {
        key: CreateItemsEnum.Table,
        label: 'Table',
        // onClick: () => setOpenModal(CreateItemsEnum.Table),
      },
      {
        key: CreateItemsEnum.Volume,
        label: 'Volume',
        // onClick: () => setOpenModal(CreateItemsEnum.Volume),
      },
      {
        key: CreateItemsEnum.Function,
        label: 'Function',
        // onClick: () => setOpenModal(CreateItemsEnum.Function),
      },
    ],
    [],
  );

  return (
    <>
      <Dropdown menu={{ items: menuItems }} trigger={['click']}>
        <Button type="primary">
          Create <DownOutlined />
        </Button>
      </Dropdown>
    </>
  );
}
