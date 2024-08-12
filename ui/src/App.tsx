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
        element: <CatalogsList />,
      },
      {
        path: '/data/:catalog',
        element: <CatalogDetails />,
      },
      {
        path: '/data/:catalog/:schema',
        element: <SchemaDetails />,
      },
      {
        path: '/data/:catalog/:schema/:table',
        element: <TableDetails />,
      },
      {
        path: '/volumes/:catalog/:schema/:volume',
        element: <VolumeDetails />,
      },
      {
        path: '/functions/:catalog/:schema/:ucFunction',
        element: <FunctionDetails />,
      },
    ],
  },
]);

function AppProvider() {
  const navigate = useNavigate();

  return (
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
            <img src="/uc-logo-reverse.png" height={32} alt="uc-logo-reverse" />
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
  );
}

function App() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { staleTime: 1000 * 5 * 60 } },
  });

  return (
    <NotificationProvider>
      <QueryClientProvider client={queryClient}>
        <RouterProvider router={router} fallbackElement={<p>Loading...</p>} />
      </QueryClientProvider>
    </NotificationProvider>
  );
}

export default App;
