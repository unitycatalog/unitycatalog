import React from 'react';
import { ConfigProvider, Layout, Menu } from 'antd';
import {
  createBrowserRouter,
  Outlet,
  RouterProvider,
  useNavigate,
} from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import SchemaBrowser from './components/SchemaBrowser';
import TableDetails from './pages/TableDetails';
import FunctionDetails from './pages/FunctionDetails';
import VolumeDetails from './pages/VolumeDetails';
import CatalogsList from './pages/CatalogsList';
import CatalogDetails from './pages/CatalogDetails';
import SchemaDetails from './pages/SchemaDetails';
import { NotificationProvider } from './utils/NotificationContext';

const router = createBrowserRouter([
  {
    element: <AppProvider />,
    children: [
      {
        path: '/',
        Component() {
          return <CatalogsList />;
        },
      },
      {
        path: '/data/:catalog',
        Component() {
          return <CatalogDetails />;
        },
      },
      {
        path: '/data/:catalog/:schema',
        Component() {
          return <SchemaDetails />;
        },
      },
      {
        path: '/data/:catalog/:schema/:table',
        Component() {
          return <TableDetails />;
        },
      },
      {
        path: '/volumes/:catalog/:schema/:volume',
        Component() {
          return <VolumeDetails />;
        },
      },
      {
        path: '/functions/:catalog/:schema/:ucFunction',
        Component() {
          return <FunctionDetails />;
        },
      },
    ],
  },
]);

function AppProvider() {
  const navigate = useNavigate();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { staleTime: 1000 * 5 * 60 } },
  });

  return (
    <QueryClientProvider client={queryClient}>
      <ConfigProvider
        theme={{
          components: {
            Typography: {
              titleMarginBottom: 0,
              titleMarginTop: 0,
            },
          },
        }}
      >
        <Layout>
          {/* Header */}
          <Layout.Header style={{ display: 'flex', alignItems: 'center' }}>
            <div style={{ marginRight: 24 }} onClick={() => navigate('/')}>
              <img
                src="/uc-logo-reverse.png"
                height={32}
                alt="uc-logo-reverse"
              />
            </div>
            <Menu
              theme="dark"
              mode="horizontal"
              defaultSelectedKeys={['catalogs']}
              items={[
                {
                  key: 'catalogs',
                  label: 'Catalogs',
                  onClick: () => navigate('/'),
                },
              ]}
              style={{ flex: 1, minWidth: 0 }}
            />
          </Layout.Header>
          {/* Content */}
          <Layout.Content
            style={{
              height: 'calc(100vh - 64px)',
              backgroundColor: '#fff',
              display: 'flex',
            }}
          >
            {/* Left: Schema Browser */}
            <div
              style={{
                width: '30%',
                minWidth: 260,
                maxWidth: 400,
                borderRight: '1px solid lightgrey',
              }}
            >
              <SchemaBrowser />
            </div>

            {/* Right: Main details content */}
            <div
              style={{
                overflowY: 'auto',
                flex: 1,
                padding: 16,
                display: 'flex',
              }}
            >
              <Outlet />
            </div>
          </Layout.Content>
        </Layout>
      </ConfigProvider>
    </QueryClientProvider>
  );
}

function App() {
  return <NotificationProvider><RouterProvider router={router} fallbackElement={<p>Loading...</p>} /></NotificationProvider>;
}

export default App;
