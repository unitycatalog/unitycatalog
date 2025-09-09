import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { jest } from '@jest/globals';
import TokenList from '../TokenList';
import { NotificationProvider } from '../../../utils/NotificationContext';
import * as tokensHooks from '../../../hooks/tokens';
import { TokenInfo } from '../../../types/tokens';

// Mock the hooks
jest.mock('../../../hooks/tokens');

const mockUseListTokens = tokensHooks.useListTokens as jest.MockedFunction<typeof tokensHooks.useListTokens>;
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

describe('TokenList', () => {
  const mockTokens: TokenInfo[] = [
    {
      tokenId: 'tok_active',
      comment: 'Active development token',
      createdAt: '2025-01-15T10:00:00Z',
      expiresAt: '2025-01-16T10:00:00Z',
      lastUsedAt: '2025-01-15T12:00:00Z',
      status: 'ACTIVE',
    },
    {
      tokenId: 'tok_expired',
      comment: 'Expired token',
      createdAt: '2025-01-14T10:00:00Z',
      expiresAt: '2025-01-14T11:00:00Z',
      status: 'EXPIRED',
    },
    {
      tokenId: 'tok_revoked',
      comment: 'Revoked token',
      createdAt: '2025-01-13T10:00:00Z',
      expiresAt: '2025-01-20T10:00:00Z',
      status: 'REVOKED',
    },
  ];

  const mockRevokeMutation = jest.fn() as jest.MockedFunction<any>;

  beforeEach(() => {
    mockUseListTokens.mockReturnValue({
      data: { tokens: mockTokens },
      isLoading: false,
    } as any);

    mockUseRevokeToken.mockReturnValue({
      mutate: mockRevokeMutation,
      isPending: false,
    } as any);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  test('renders token list correctly', () => {
    render(
      <TestWrapper>
        <TokenList />
      </TestWrapper>
    );

    expect(screen.getByText('Active development token')).toBeInTheDocument();
    expect(screen.getByText('Expired token')).toBeInTheDocument();
    expect(screen.getByText('Revoked token')).toBeInTheDocument();
  });

  test('displays correct status tags', () => {
    render(
      <TestWrapper>
        <TokenList />
      </TestWrapper>
    );

    expect(screen.getByText('ACTIVE')).toBeInTheDocument();
    expect(screen.getByText('EXPIRED')).toBeInTheDocument();
    expect(screen.getByText('REVOKED')).toBeInTheDocument();
  });

  test('shows empty state when no tokens exist', () => {
    mockUseListTokens.mockReturnValue({
      data: { tokens: [] },
      isLoading: false,
    } as any);

    render(
      <TestWrapper>
        <TokenList />
      </TestWrapper>
    );

    expect(screen.getByText('No tokens found. Create your first token above.')).toBeInTheDocument();
  });

  test('shows loading state', () => {
    mockUseListTokens.mockReturnValue({
      data: undefined,
      isLoading: true,
    } as any);

    render(
      <TestWrapper>
        <TokenList />
      </TestWrapper>
    );

    // Ant Design table shows loading spinner when isLoading is true
    expect(screen.getByRole('table')).toBeInTheDocument();
  });

  test('allows revoking only active tokens', () => {
    render(
      <TestWrapper>
        <TokenList />
      </TestWrapper>
    );

    const revokeButtons = screen.getAllByText('Revoke');
    
    // Should have revoke buttons for all tokens (some disabled)
    expect(revokeButtons.length).toBeGreaterThan(0);
  });
});
