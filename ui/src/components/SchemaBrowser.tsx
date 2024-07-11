import React, { useEffect, useState } from 'react';
import { useListCatalogs } from '../hooks/catalog';
import { Tree, TreeDataNode, Typography } from 'antd';
import {
  DatabaseOutlined,
  DownOutlined,
  LoadingOutlined,
  ProductOutlined,
} from '@ant-design/icons';
import { useListSchemas } from '../hooks/schemas';
import { useNavigate } from 'react-router-dom';

export default function SchemaBrowser() {
  const navigate = useNavigate();
  const [catalogToExpand, setCatalogToExpand] = useState<string>();
  const [treeData, setTreeData] = useState<TreeDataNode[]>([]);

  const listCatalogsRequest = useListCatalogs();
  const listSchemasRequest = useListSchemas({
    catalog: catalogToExpand!,
    options: { enabled: !!catalogToExpand },
  });

  useEffect(() => {
    setTreeData(
      listCatalogsRequest.data?.catalogs.map((catalog) => {
        const catalogNode: TreeDataNode = {
          title: (
            <>
              <ProductOutlined /> {catalog.name}
            </>
          ),
          key: catalog.name,
          children: [
            {
              title: <LoadingOutlined />,
              key: `${catalog.id}:no-data`,
              isLeaf: true,
              style: { color: 'gray' },
            },
          ],
        };
        return catalogNode;
      }) ?? []
    );
  }, [listCatalogsRequest.data?.catalogs]);

  useEffect(() => {
    setTreeData((treeData) => {
      const catalogNode = treeData.find(({ key }) => key === catalogToExpand);
      if (catalogNode) {
        const schemas = listSchemasRequest.data?.schemas;
        catalogNode.children = schemas?.length
          ? schemas.map(({ catalog_name, name }) => ({
              title: (
                <>
                  <DatabaseOutlined /> {name}
                </>
              ),
              key: `${catalog_name}.${name}`,
              isLeaf: false,
              children: [
                {
                  title: 'Tables',
                  key: `${catalog_name}.${name}:tables`,
                  isLeaf: false,
                  selectable: false,
                },
                {
                  title: 'Volumes',
                  key: `${catalog_name}.${name}:volumes`,
                  isLeaf: false,
                  selectable: false,
                },
                {
                  title: 'Functions',
                  key: `${catalog_name}.${name}:functions`,
                  isLeaf: false,
                  selectable: false,
                },
              ],
            }))
          : [
              {
                title: 'No schemas found',
                key: `${catalogToExpand}:no-data`,
                isLeaf: true,
                style: { color: 'gray' },
                selectable: false,
              },
            ];
      }
      return [...treeData];
    });
  }, [catalogToExpand, listSchemasRequest.data?.schemas]);

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
        <Tree
          showLine
          switcherIcon={<DownOutlined />}
          treeData={treeData}
          onExpand={(_, { expanded, node }) => {
            if (!expanded) return;
            const [entityName, entityType] = (node.key as string)?.split(':');
            const [catalogName, schemaName] = entityName?.split('.');
            if (entityType) {
              // TODO: expand entity
            } else if (schemaName) {
              // TODO: expand schema
            } else if (catalogName) {
              setCatalogToExpand(catalogName);
            }
          }}
          onClick={(_, { key }) => {
            const [entityName, entityType] = (key as string)?.split(':');
            if (entityType === 'no-data') return;
            const [catalogName, schemaName] = entityName?.split('.');
            if (entityType) {
              // TODO: navigate to entity
            } else if (schemaName) {
              navigate(`/data/${catalogName}/${schemaName}`);
            } else if (catalogName) {
              navigate(`/data/${catalogName}`);
            }
          }}
        />
      </div>
    </div>
  );
}
