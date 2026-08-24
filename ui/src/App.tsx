import React, { useMemo } from 'react';
import {
  Avatar,
  ConfigProvider,
  Dropdown,
  Layout,
  Menu,
  MenuProps,
  Typography,
} from 'antd';
import {
  createBrowserRouter,
  Outlet,
  RouterProvider,
  useNavigate,
} from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { QUERY_STALE_TIME } from './utils/constants';

import SchemaBrowser from './components/SchemaBrowser';
import TableDetails from './pages/TableDetails';
import FunctionDetails from './pages/FunctionDetails';
import VolumeDetails from './pages/VolumeDetails';
import CatalogsList from './pages/CatalogsList';
import CatalogDetails from './pages/CatalogDetails';
import SchemaDetails from './pages/SchemaDetails';
import { NotificationProvider } from './utils/NotificationContext';
import ModelDetails from './pages/ModelDetails';
import Login from './pages/Login';
import { AuthProvider, useAuth } from './context/auth-context';
import { UserOutlined } from '@ant-design/icons';
import ModelVersionDetails from './pages/ModelVersionDetails';

// TODO:
// As of [19/02/2025], this implementation should be updated once the following PR are merged.
// SEE:
// https://github.com/unitycatalog/unitycatalog/pull/809
const authEnabled = process.env.REACT_APP_GOOGLE_AUTH_ENABLED === 'true';

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
      {
        path: '/models/:catalog/:schema/:model',
        element: <ModelDetails />,
      },
      {
        path: '/models/:catalog/:schema/:model/versions/:version',
        element: <ModelVersionDetails />,
      },
    ],
  },
]);

function AppProvider() {
  const { logout, currentUser } = useAuth();
  const navigate = useNavigate();

  const profileMenuItems = useMemo(
    (): MenuProps['items'] => [
      {
        key: 'userInfo',
        label: (
          <div
            style={{
              display: 'flex',
              flexDirection: 'column',
              cursor: 'default',
            }}
          >
            <Typography.Text>{currentUser?.displayName}</Typography.Text>
            <Typography.Text>{currentUser?.emails?.[0]?.value}</Typography.Text>
          </div>
        ),
      },
      {
        type: 'divider',
      },
      {
        key: 'logout',
        label: 'Log out',
        onClick: () => logout().then(() => navigate('/')),
      },
    ],
    [currentUser, logout, navigate],
  );

  return authEnabled && !currentUser ? (
    <Login />
  ) : (
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
        <Layout.Header
          style={{
            display: 'flex',
            alignItems: 'center',
            width: '100%',
            justifyContent: 'space-between',
          }}
        >
          <div
            style={{ display: 'flex', alignItems: 'center', flex: '1 0 auto' }}
          >
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
          </div>
          {authEnabled && (
            <div>
              <Dropdown
                menu={{ items: profileMenuItems }}
                trigger={['click']}
                placement={'bottomRight'}
              >
                <Avatar
                  icon={<UserOutlined />}
                  style={{
                    backgroundColor: 'white',
                    color: 'black',
                    cursor: 'pointer',
                  }}
                />
              </Dropdown>
            </div>
          )}
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
    defaultOptions: { queries: { staleTime: QUERY_STALE_TIME } },
  });

  return authEnabled ? (
    <NotificationProvider>
      <QueryClientProvider client={queryClient}>
        <AuthProvider>
          <RouterProvider router={router} fallbackElement={<p>Loading...</p>} />
        </AuthProvider>
      </QueryClientProvider>
    </NotificationProvider>
  ) : (
    <NotificationProvider>
      <QueryClientProvider client={queryClient}>
        <RouterProvider router={router} fallbackElement={<p>Loading...</p>} />
      </QueryClientProvider>
    </NotificationProvider>
  );
}

export default App;
