import React from 'react';
import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { AdminPanel } from '../pages/AdminPanel';

// Mock all dependencies
jest.mock('../context/auth-context', () => ({
  useAuth: jest.fn(),
}));

jest.mock('../hooks/adminStatus', () => ({
  useAdminStatus: jest.fn(),
}));

jest.mock('../hooks/bootstrap', () => ({
  useBootstrapStatus: jest.fn(),
}));

jest.mock('../components/BootstrapFlow', () => ({
  BootstrapFlow: () => <div data-testid="bootstrap-flow">Bootstrap Flow</div>,
}));

jest.mock('../components/admin/UsersTab', () => ({
  UsersTab: () => <div data-testid="users-tab">Users Tab</div>,
}));

jest.mock('../components/admin/PermissionsTab', () => ({
  PermissionsTab: () => <div data-testid="permissions-tab">Permissions Tab</div>,
}));

import { useAuth } from '../context/auth-context';
import { useAdminStatus } from '../hooks/adminStatus';
import { useBootstrapStatus } from '../hooks/bootstrap';

const mockUseAuth = useAuth as jest.MockedFunction<typeof useAuth>;
const mockUseAdminStatus = useAdminStatus as jest.MockedFunction<typeof useAdminStatus>;
const mockUseBootstrapStatus = useBootstrapStatus as jest.MockedFunction<typeof useBootstrapStatus>;

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

describe('AdminPanel with Bootstrap Integration', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('shows bootstrap flow when available and needed', () => {
    mockUseAuth.mockReturnValue({
      currentUser: { id: 'user-1', displayName: 'Test User' },
    } as any);
    
    mockUseAdminStatus.mockReturnValue({
      data: { isAdmin: false, canManageUsers: false, canManagePermissions: false },
      isLoading: false,
      refetch: jest.fn(),
    } as any);
    
    mockUseBootstrapStatus.mockReturnValue({
      data: { available: true, needsBootstrap: true },
      isLoading: false,
    } as any);

    renderWithQueryClient(<AdminPanel />);

    expect(screen.getByTestId('bootstrap-flow')).toBeInTheDocument();
  });

  it('shows admin panel when user has admin privileges', () => {
    mockUseAuth.mockReturnValue({
      currentUser: { id: 'user-1', displayName: 'Admin User' },
    } as any);
    
    mockUseAdminStatus.mockReturnValue({
      data: { isAdmin: true, canManageUsers: true, canManagePermissions: true },
      isLoading: false,
      refetch: jest.fn(),
    } as any);
    
    mockUseBootstrapStatus.mockReturnValue({
      data: { available: false, needsBootstrap: false },
      isLoading: false,
    } as any);

    renderWithQueryClient(<AdminPanel />);

    expect(screen.getByText('Admin Panel')).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: /Users/ })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: /Permissions/ })).toBeInTheDocument();
  });

  it('shows access denied when no admin privileges and no bootstrap', () => {
    mockUseAuth.mockReturnValue({
      currentUser: { id: 'user-1', displayName: 'Regular User' },
    } as any);
    
    mockUseAdminStatus.mockReturnValue({
      data: { isAdmin: false, canManageUsers: false, canManagePermissions: false },
      isLoading: false,
      refetch: jest.fn(),
    } as any);
    
    mockUseBootstrapStatus.mockReturnValue({
      data: { available: false, needsBootstrap: false },
      isLoading: false,
    } as any);

    renderWithQueryClient(<AdminPanel />);

    expect(screen.getByText('Admin Access Required')).toBeInTheDocument();
    expect(screen.getByText(/Contact your Unity Catalog administrator/)).toBeInTheDocument();
  });
});
