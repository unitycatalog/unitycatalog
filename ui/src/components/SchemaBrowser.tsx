import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Tree, TreeDataNode, Typography } from 'antd';
import {
  DatabaseOutlined,
  DownOutlined,
  LoadingOutlined,
  ProductOutlined,
} from '@ant-design/icons';

import { updateEntityTreeData } from '../utils/schemaBrowser';
import { useListCatalogs } from '../hooks/catalog';
import { useListSchemas } from '../hooks/schemas';
import { useListTables } from '../hooks/tables';
import { useListVolumes } from '../hooks/volumes';
import { useListFunctions } from '../hooks/functions';

export default function SchemaBrowser() {
  const navigate = useNavigate();
  const [catalogToExpand, setCatalogToExpand] = useState<string>();
  const [entityToExpand, setEntityToExpand] = useState<{
    catalog: string;
    schema: string;
    type: string;
  }>({ catalog: '', schema: '', type: '' });

  const [treeData, setTreeData] = useState<TreeDataNode[]>([]);

  const listCatalogsRequest = useListCatalogs();
  const listSchemasRequest = useListSchemas({
    catalog: catalogToExpand!,
    options: { enabled: !!catalogToExpand },
  });

  const listTablesRequest = useListTables({
    catalog: entityToExpand.catalog,
    schema: entityToExpand.schema,
    options: {
      enabled:
        !!entityToExpand.catalog &&
        !!entityToExpand.schema &&
        entityToExpand.type === 'tables',
    },
  });

  const listVolumesRequest = useListVolumes({
    catalog: entityToExpand.catalog,
    schema: entityToExpand.schema,
    options: {
      enabled:
        !!entityToExpand.catalog &&
        !!entityToExpand.schema &&
        entityToExpand.type === 'volumes',
    },
  });

  const listFunctionsRequest = useListFunctions({
    catalog: entityToExpand.catalog,
    schema: entityToExpand.schema,
    options: {
      enabled:
        !!entityToExpand.catalog &&
        !!entityToExpand.schema &&
        entityToExpand.type === 'functions',
    },
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

  useEffect(() => {
    setTreeData((treeData) => {
      return updateEntityTreeData({
        treeData,
        entityToExpand,
        entityList: listTablesRequest.data?.tables ?? [],
      });
    });
  }, [entityToExpand, listTablesRequest.data?.tables]);

  useEffect(() => {
    setTreeData((treeData) => {
      return updateEntityTreeData({
        treeData,
        entityToExpand,
        entityList: listVolumesRequest.data?.volumes ?? [],
      });
    });
  }, [entityToExpand, listVolumesRequest.data?.volumes]);

  useEffect(() => {
    setTreeData((treeData) => {
      return updateEntityTreeData({
        treeData,
        entityToExpand,
        entityList: listFunctionsRequest.data?.functions ?? [],
      });
    });
  }, [entityToExpand, listFunctionsRequest.data?.functions]);

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
              if (schemaName && catalogName) {
                setEntityToExpand({
                  catalog: catalogName,
                  schema: schemaName,
                  type: entityType,
                });
              }
            } else if (!schemaName && catalogName) {
              setCatalogToExpand(catalogName);
            }
          }}
          onClick={(_, { key }) => {
            const [entityFullName, entityType] = (key as string)?.split(':');
            if (entityType === 'no-data') return;
            const [catalogName, schemaName, entityName] =
              entityFullName?.split('.');
            if (entityType) {
              switch (entityType) {
                case 'tables':
                  navigate(`/data/${catalogName}/${schemaName}/${entityName}`);
                  break;
                case 'volumes':
                  navigate(
                    `/volumes/${catalogName}/${schemaName}/${entityName}`
                  );
                  break;
                case 'functions':
                  navigate(
                    `/functions/${catalogName}/${schemaName}/${entityName}`
                  );
                  break;
              }
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
