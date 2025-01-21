import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Tree, TreeDataNode, Typography } from 'antd';
import {
  DatabaseOutlined,
  DownOutlined,
  LoadingOutlined,
  ProductOutlined,
} from '@ant-design/icons';
import { Key } from 'antd/es/table/interface';

import { updateEntityTreeData } from '../utils/schemaBrowser';
import { useListCatalogs } from '../hooks/catalog';
import { useListSchemas } from '../hooks/schemas';
import { useListTables } from '../hooks/tables';
import { useListVolumes } from '../hooks/volumes';
import { useListFunctions } from '../hooks/functions';
import { useListModels } from '../hooks/models';
import { SchemaTabs } from '../pages/SchemaDetails';

export default function SchemaBrowser() {
  const navigate = useNavigate();
  const [expendedKeys, setExpendedKeys] = useState<Key[]>([]);
  const [catalogToExpand, setCatalogToExpand] = useState<string>();
  const [entityToExpand, setEntityToExpand] = useState<{
    catalog: string;
    schema: string;
    type: string;
  }>({ catalog: '', schema: '', type: '' });

  const [treeData, setTreeData] = useState<TreeDataNode[]>([]);

  const listCatalogsRequest = useListCatalogs();
  const listSchemasRequest = useListSchemas({
    catalog_name: catalogToExpand!,
    options: { enabled: !!catalogToExpand },
  });

  const listTablesRequest = useListTables({
    catalog_name: entityToExpand.catalog,
    schema_name: entityToExpand.schema,
    options: {
      enabled:
        !!entityToExpand.catalog &&
        !!entityToExpand.schema &&
        entityToExpand.type === 'tables',
    },
  });

  const listVolumesRequest = useListVolumes({
    catalog_name: entityToExpand.catalog,
    schema_name: entityToExpand.schema,
    options: {
      enabled:
        !!entityToExpand.catalog &&
        !!entityToExpand.schema &&
        entityToExpand.type === 'volumes',
    },
  });

  const listFunctionsRequest = useListFunctions({
    catalog_name: entityToExpand.catalog,
    schema_name: entityToExpand.schema,
    options: {
      enabled:
        !!entityToExpand.catalog &&
        !!entityToExpand.schema &&
        entityToExpand.type === 'functions',
    },
  });

  const listModelsRequest = useListModels({
    catalog_name: entityToExpand.catalog,
    schema_name: entityToExpand.schema,
    options: {
      enabled:
        !!entityToExpand.catalog &&
        !!entityToExpand.schema &&
        entityToExpand.type === 'registered_models',
    },
  });

  useEffect(() => {
    setExpendedKeys([]); // Collapse all nodes when list catalog updates
    setTreeData(
      listCatalogsRequest.data?.catalogs?.map((catalog) => {
        const catalogNode: TreeDataNode = {
          title: (
            <>
              <ProductOutlined /> {catalog.name}
            </>
          ),
          key: `${catalog.name}`,
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
      }) ?? [],
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
                  selectable: true,
                },
                {
                  title: 'Volumes',
                  key: `${catalog_name}.${name}:volumes`,
                  isLeaf: false,
                  selectable: true,
                },
                {
                  title: 'Functions',
                  key: `${catalog_name}.${name}:functions`,
                  isLeaf: false,
                  selectable: true,
                },
                {
                  title: 'Models',
                  key: `${catalog_name}.${name}:registered_models`,
                  isLeaf: false,
                  selectable: true,
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
    if (entityToExpand.type === 'tables') {
      const entityList = listTablesRequest.data?.tables ?? [];
      setTreeData((treeData) =>
        updateEntityTreeData({ treeData, entityToExpand, entityList }),
      );
    }
  }, [entityToExpand, listTablesRequest.data?.tables]);

  useEffect(() => {
    if (entityToExpand.type === 'volumes') {
      const entityList = listVolumesRequest.data?.volumes ?? [];
      setTreeData((treeData) =>
        updateEntityTreeData({ treeData, entityToExpand, entityList }),
      );
    }
  }, [entityToExpand, listVolumesRequest.data?.volumes]);

  useEffect(() => {
    if (entityToExpand.type === 'functions') {
      const entityList = listFunctionsRequest.data?.functions ?? [];
      setTreeData((treeData) =>
        updateEntityTreeData({ treeData, entityToExpand, entityList }),
      );
    }
  }, [entityToExpand, listFunctionsRequest.data?.functions]);

  useEffect(() => {
    if (entityToExpand.type === 'registered_models') {
      const entityList = listModelsRequest.data?.registered_models ?? [];
      setTreeData((treeData) =>
        updateEntityTreeData({ treeData, entityToExpand, entityList }),
      );
    }
  }, [entityToExpand, listModelsRequest.data?.registered_models]);

  return (
    <div style={{ height: '100%', overflowY: 'auto' }}>
      <Typography.Title
        level={4}
        style={{
          padding: '8px 12px',
          position: 'sticky',
          top: 0,
          background: 'white',
          zIndex: 1,
        }}
      >
        Browse
      </Typography.Title>

      <div style={{ margin: '0 12px' }}>
        <Tree
          showLine
          switcherIcon={<DownOutlined />}
          treeData={treeData}
          expandedKeys={expendedKeys}
          onExpand={(keys, { expanded, node }) => {
            setExpendedKeys(keys);
            if (!expanded) return;
            const [fullName, type] = (node.key as string)?.split(':');
            const [catalog, schema] = fullName?.split('.');
            if (type) {
              if (schema && catalog) {
                setEntityToExpand({ catalog, schema, type });
              }
            } else if (!schema && catalog) {
              setCatalogToExpand(catalog);
            }
          }}
          onClick={(_, { key }) => {
            const [entityFullName, type] = (key as string)?.split(':');
            if (type === 'no-data') return;
            const [catalog, schema, entity] = entityFullName?.split('.');
            if (type && !entity) {
              switch (type) {
                case 'tables':
                  return navigate(`/data/${catalog}/${schema}`, {
                    state: { tab: SchemaTabs.Tables },
                  });
                case 'volumes':
                  return navigate(`/data/${catalog}/${schema}`, {
                    state: { tab: SchemaTabs.Volumes },
                  });
                case 'functions':
                  return navigate(`/data/${catalog}/${schema}`, {
                    state: { tab: SchemaTabs.Functions },
                  });
                case 'registered_models':
                  return navigate(`/data/${catalog}/${schema}`, {
                    state: { tab: SchemaTabs.Models },
                  });
              }
            } else if (type && entity) {
              switch (type) {
                case 'tables':
                  return navigate(`/data/${catalog}/${schema}/${entity}`);
                case 'volumes':
                  return navigate(`/volumes/${catalog}/${schema}/${entity}`);
                case 'functions':
                  return navigate(`/functions/${catalog}/${schema}/${entity}`);
                case 'registered_models':
                  return navigate(`/models/${catalog}/${schema}/${entity}`);
              }
            } else if (schema) {
              navigate(`/data/${catalog}/${schema}`);
            } else if (catalog) {
              navigate(`/data/${catalog}`);
            }
          }}
        />
      </div>
    </div>
  );
}
