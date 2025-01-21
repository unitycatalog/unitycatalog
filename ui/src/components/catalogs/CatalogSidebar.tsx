import { Typography } from 'antd';
import { CatalogInterface, useGetCatalog } from '../../hooks/catalog';
import { formatTimestamp } from '../../utils/formatTimestamp';
import MetadataList, { MetadataListType } from '../MetadataList';

interface CatalogSidebarProps {
  catalog: string;
}

const CATALOG_METADATA: MetadataListType<CatalogInterface> = [
  {
    key: 'created_at',
    label: 'Created at',
    dataIndex: 'created_at',
    render: (value) => (
      <Typography.Text>{formatTimestamp(value)}</Typography.Text>
    ),
  },
  {
    key: 'updated_at',
    label: 'Updated at',
    dataIndex: 'updated_at',
    render: (value) => (
      <Typography.Text>{formatTimestamp(value)}</Typography.Text>
    ),
  },
];

export default function CatalogSidebar({ catalog }: CatalogSidebarProps) {
  const { data } = useGetCatalog({ name: catalog });

  if (!data) return null;

  return (
    <MetadataList
      data={data}
      metadata={CATALOG_METADATA}
      title="Catalog details"
    />
  );
}
