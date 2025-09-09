import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { RequireAuth } from '../components/RequireAuth';

// Mock the entire MSAL context module
jest.mock('../context/msal-auth-context', () => ({
  MsalAuthProvider: ({ children }: any) => children,
  useMsalAuth: () => ({
    isAuthenticated: false,
    account: null,
    login: jest.fn(),
    logout: jest.fn(),
    getIdToken: jest.fn(),
  }),
}));

// Mock MSAL React
jest.mock('@azure/msal-react', () => ({
  MsalProvider: ({ children }: any) => children,
  useMsal: () => ({
    instance: {
      loginPopup: jest.fn(),
      logoutPopup: jest.fn(),
      acquireTokenSilent: jest.fn(),
    },
    accounts: [],
  }),
  useAccount: () => null,
}));

// Mock auth hooks
jest.mock('../hooks/user', () => ({
  useGetCurrentUser: () => ({ data: null, refetch: jest.fn() }),
  useLoginWithToken: () => ({ mutate: jest.fn() }),
  useLogoutCurrentUser: () => ({ mutate: jest.fn() }),
}));

// Mock notification context
jest.mock('../utils/NotificationContext', () => ({
  useNotification: () => ({ setNotification: jest.fn() }),
}));

// Mock auth context
jest.mock('../context/auth-context', () => ({
  AuthProvider: ({ children }: any) => children,
  useAuth: () => ({
    currentUser: null,
    loginWithToken: jest.fn(),
    logout: jest.fn(),
  }),
}));

const createWrapper = (initialEntries = ['/']) => {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });

  return ({ children }: { children: React.ReactNode }) => (
    <MemoryRouter initialEntries={initialEntries}>
      <QueryClientProvider client={queryClient}>
        {children}
      </QueryClientProvider>
    </MemoryRouter>
  );
};

describe('Authentication Flow', () => {
  beforeEach(() => {
    // Reset environment variables
    process.env.REACT_APP_AZURE_CLIENT_ID = 'test-client-id';
    process.env.REACT_APP_AZURE_AUTHORITY = 'https://login.microsoftonline.com/test-tenant';
  });

  test('RequireAuth redirects to login when unauthenticated', async () => {
    const TestComponent = () => <div>Protected Content</div>;

    render(
      <RequireAuth>
        <TestComponent />
      </RequireAuth>,
      { wrapper: createWrapper(['/protected']) }
    );

    // Wait for navigation to happen
    await waitFor(() => {
      // Check that we're redirected to login
      expect(screen.queryByText('Protected Content')).not.toBeInTheDocument();
    });
  });

  test('RequireAuth shows loading when authenticated but no user data', async () => {
    // Mock authenticated state but no user data
    const mockUseMsalAuth = require('../context/msal-auth-context').useMsalAuth;
    mockUseMsalAuth.mockReturnValue({
      isAuthenticated: true,
      account: { username: 'test@example.com' },
      login: jest.fn(),
      logout: jest.fn(),
      getIdToken: jest.fn(),
    });
    
    const TestComponent = () => <div>Protected Content</div>;

    render(
      <RequireAuth>
        <TestComponent />
      </RequireAuth>,
      { wrapper: createWrapper() }
    );

    // Should show loading spinner
    expect(screen.getByRole('img', { name: 'loading' })).toBeInTheDocument();
  });
});
