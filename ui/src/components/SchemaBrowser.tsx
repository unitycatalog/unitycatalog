import React, { useEffect, useMemo, useState } from 'react';
import { useListCatalogs } from '../hooks/catalog';
import { Tree, TreeDataNode, Typography } from 'antd';
import { DownOutlined } from '@ant-design/icons';

export default function SchemaBrowser() {
  const listCatalogsRequest = useListCatalogs();
  const [treeData, setTreeData] = useState<TreeDataNode[]>([]);

  useEffect(() => {
    setTreeData(
      listCatalogsRequest.data?.catalogs.map((catalog) => {
        const catalogNode: TreeDataNode = {
          title: catalog.name,
          key: catalog.id,
          children: [
            {
              title: 'TODO',
              key: `${catalog.id}-no-data`,
              isLeaf: true,
            },
          ],
        };
        return catalogNode;
      }) ?? []
    );
  }, [listCatalogsRequest.data?.catalogs]);

  return (
    <div>
      <Typography.Title
        level={4}
        style={{
          padding: '8px 12px',
        }}
      >
        Browse
      </Typography.Title>

      <div style={{ margin: '0 12px' }}>
        <Tree showLine switcherIcon={<DownOutlined />} treeData={treeData} />
      </div>
    </div>
  );
}
