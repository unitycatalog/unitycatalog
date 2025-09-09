import React from 'react';
import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

// Mock all the dependencies before importing AdminPanel
jest.mock('../context/auth-context', () => ({
  useAuth: jest.fn(),
}));

jest.mock('../context/msal-auth-context', () => ({
  useMsalAuth: jest.fn(),
}));

jest.mock('../hooks/admin', () => ({
  useListUsers: () => ({ data: [], isLoading: false, refetch: jest.fn() }),
  useCreateUser: () => ({ mutateAsync: jest.fn(), isPending: false }),
  useUpdateUser: () => ({ mutateAsync: jest.fn(), isPending: false }),
  useDeleteUser: () => ({ mutateAsync: jest.fn(), isPending: false }),
  useGetPermissions: () => ({ data: null, isLoading: false, refetch: jest.fn() }),
  useUpdatePermissions: () => ({ mutateAsync: jest.fn(), isPending: false }),
}));

jest.mock('../components/admin/UsersTab', () => ({
  UsersTab: () => <div data-testid="users-tab">Users Tab</div>,
}));

jest.mock('../components/admin/PermissionsTab', () => ({
  PermissionsTab: () => <div data-testid="permissions-tab">Permissions Tab</div>,
}));

// Now import AdminPanel after mocking dependencies
import { AdminPanel } from '../pages/AdminPanel';
import { useAuth } from '../context/auth-context';
import { useMsalAuth } from '../context/msal-auth-context';

const mockUseAuth = useAuth as jest.MockedFunction<typeof useAuth>;
const mockUseMsalAuth = useMsalAuth as jest.MockedFunction<typeof useMsalAuth>;

const renderWithQueryClient = (component: React.ReactNode) => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      {component}
    </QueryClientProvider>
  );
};

describe('AdminPanel', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('shows access denied when no admin token', () => {
    mockUseAuth.mockReturnValue({
      currentUser: null,
      loginWithToken: jest.fn(),
      logout: jest.fn(),
    } as any);
    mockUseMsalAuth.mockReturnValue({
      account: null,
      login: jest.fn(),
      logout: jest.fn(),
      getIdToken: jest.fn(),
      isAuthenticated: false,
    } as any);

    renderWithQueryClient(<AdminPanel />);

    expect(screen.getByText('Admin Access Required')).toBeInTheDocument();
    expect(screen.getByText(/Admin token required or insufficient privileges/)).toBeInTheDocument();
  });

  it('renders admin panel with tabs when admin token is present', () => {
    mockUseAuth.mockReturnValue({
      currentUser: {
        id: 'user-1',
        displayName: 'Admin User',
        emails: [{ value: 'admin@test.com', primary: true }],
        active: true,
        meta: { created: '2023-01-01' },
      },
      loginWithToken: jest.fn(),
      logout: jest.fn(),
    } as any);
    mockUseMsalAuth.mockReturnValue({
      account: {
        username: 'admin@test.com',
        name: 'Admin User',
        localAccountId: 'account-1',
        tenantId: 'tenant-1',
      },
      login: jest.fn(),
      logout: jest.fn(),
      getIdToken: jest.fn(),
      isAuthenticated: true,
    } as any);

    renderWithQueryClient(<AdminPanel />);

    expect(screen.getByText('Admin Panel')).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: /Users/ })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: /Permissions/ })).toBeInTheDocument();
  });
});
