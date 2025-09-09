import React from 'react';
import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { jest } from '@jest/globals';
import DeveloperTokens from '../DeveloperTokens';
import { NotificationProvider } from '../../utils/NotificationContext';
import * as tokensHooks from '../../hooks/tokens';

// Mock the hooks
jest.mock('../../hooks/tokens');

const mockUseListTokens = tokensHooks.useListTokens as jest.MockedFunction<typeof tokensHooks.useListTokens>;
const mockUseCreateToken = tokensHooks.useCreateToken as jest.MockedFunction<typeof tokensHooks.useCreateToken>;
const mockUseRevokeToken = tokensHooks.useRevokeToken as jest.MockedFunction<typeof tokensHooks.useRevokeToken>;

// Test wrapper
const TestWrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } }
  });
  
  return (
    <QueryClientProvider client={queryClient}>
      <NotificationProvider>
        {children}
      </NotificationProvider>
    </QueryClientProvider>
  );
};

describe('DeveloperTokens', () => {
  beforeEach(() => {
    mockUseListTokens.mockReturnValue({
      data: { tokens: [] },
      isLoading: false,
    } as any);

    mockUseCreateToken.mockReturnValue({
      mutateAsync: jest.fn(),
      isPending: false,
    } as any);

    mockUseRevokeToken.mockReturnValue({
      mutate: jest.fn(),
      isPending: false,
    } as any);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  test('renders page title and description', () => {
    render(
      <TestWrapper>
        <DeveloperTokens />
      </TestWrapper>
    );

    expect(screen.getByText('Developer Tokens')).toBeInTheDocument();
    expect(screen.getByText(/Create and manage personal access tokens/)).toBeInTheDocument();
  });

  test('renders security notice banner', () => {
    render(
      <TestWrapper>
        <DeveloperTokens />
      </TestWrapper>
    );

    expect(screen.getByText('Token Security Notice')).toBeInTheDocument();
    expect(screen.getByText(/Tokens adopt your current permissions/)).toBeInTheDocument();
    expect(screen.getByText(/Token values are displayed only once/)).toBeInTheDocument();
  });

  test('renders create token form and token list', () => {
    render(
      <TestWrapper>
        <DeveloperTokens />
      </TestWrapper>
    );

    expect(screen.getByText('Create New Token')).toBeInTheDocument();
    expect(screen.getByText('Your Tokens')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Create Token/ })).toBeInTheDocument();
  });

  test('shows responsive layout', () => {
    render(
      <TestWrapper>
        <DeveloperTokens />
      </TestWrapper>
    );

    // Check that the layout uses Ant Design Row/Col structure
    const createTokenCard = screen.getByText('Create New Token').closest('.ant-card');
    const tokenListCard = screen.getByText('Your Tokens').closest('.ant-card');
    
    expect(createTokenCard).toBeInTheDocument();
    expect(tokenListCard).toBeInTheDocument();
  });
});
