import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { jest } from '@jest/globals';
import CreateTokenForm from '../CreateTokenForm';
import { NotificationProvider } from '../../../utils/NotificationContext';
import * as tokensHooks from '../../../hooks/tokens';

// Mock the hooks
jest.mock('../../../hooks/tokens');

const mockUseCreateToken = tokensHooks.useCreateToken as jest.MockedFunction<typeof tokensHooks.useCreateToken>;

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

describe('CreateTokenForm', () => {
  const mockMutateAsync = jest.fn() as jest.MockedFunction<any>;

  beforeEach(() => {
    mockUseCreateToken.mockReturnValue({
      mutateAsync: mockMutateAsync,
      isPending: false,
    } as any);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  test('renders form fields correctly', () => {
    render(
      <TestWrapper>
        <CreateTokenForm />
      </TestWrapper>
    );

    expect(screen.getByText(/Token Description/)).toBeInTheDocument();
    expect(screen.getByText(/Token Lifetime/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Create Token/ })).toBeInTheDocument();
  });

  test('creates token successfully and displays once', async () => {
    const mockToken = {
      tokenId: 'tok_123',
      tokenValue: 'dapi_abcd1234567890',
      comment: 'Test token',
      createdAt: '2025-01-15T10:00:00Z',
      expiresAt: '2025-01-16T10:00:00Z',
    };

    mockMutateAsync.mockResolvedValueOnce(mockToken);

    render(
      <TestWrapper>
        <CreateTokenForm />
      </TestWrapper>
    );

    // Submit form with default values
    fireEvent.click(screen.getByRole('button', { name: /Create Token/ }));

    await waitFor(() => {
      expect(mockMutateAsync).toHaveBeenCalledWith({
        lifetimeSeconds: 86400, // Default 24 hours
      });
    });

    // Check token display modal
    await waitFor(() => {
      expect(screen.getByText('Token Created Successfully')).toBeInTheDocument();
      expect(screen.getByText('dapi_abcd1234567890')).toBeInTheDocument();
    });
  });

  test('handles token creation error', async () => {
    mockMutateAsync.mockRejectedValueOnce(new Error('Token creation failed'));

    render(
      <TestWrapper>
        <CreateTokenForm />
      </TestWrapper>
    );

    fireEvent.click(screen.getByRole('button', { name: /Create Token/ }));

    await waitFor(() => {
      expect(mockMutateAsync).toHaveBeenCalled();
    });

    // Error should be handled by the mutation's onError callback
  });

  test('shows token permissions banner', () => {
    render(
      <TestWrapper>
        <CreateTokenForm />
      </TestWrapper>
    );

    expect(screen.getByText(/Token Permissions/)).toBeInTheDocument();
    expect(screen.getByText(/inherit your current permissions/)).toBeInTheDocument();
  });
});
