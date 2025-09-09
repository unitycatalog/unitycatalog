import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { jest } from '@jest/globals';
import { useCreateToken, useListTokens, useRevokeToken } from '../tokens';
import { NotificationProvider } from '../../utils/NotificationContext';

// Mock the CLIENT
jest.mock('../../context/client', () => ({
  CLIENT: {
    post: jest.fn(),
    get: jest.fn(),
    delete: jest.fn(),
  },
}));

import { CLIENT } from '../../context/client';
const mockCLIENT = CLIENT as jest.Mocked<typeof CLIENT>;

// Test wrapper
const createWrapper = () => {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } }
  });
  
  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>
      <NotificationProvider>
        {children}
      </NotificationProvider>
    </QueryClientProvider>
  );
};

describe('Token Hooks', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('useCreateToken', () => {
    test('creates token successfully', async () => {
      const mockResponse = {
        data: {
          tokenId: 'tok_123',
          tokenValue: 'dapi_abcd1234567890',
          comment: 'Test token',
          createdAt: '2025-01-15T10:00:00Z',
          expiresAt: '2025-01-16T10:00:00Z',
        }
      };

      mockCLIENT.post.mockResolvedValueOnce(mockResponse);

      const { result } = renderHook(() => useCreateToken(), {
        wrapper: createWrapper(),
      });

      const createTokenRequest = {
        comment: 'Test token',
        lifetimeSeconds: 3600,
      };

      await waitFor(async () => {
        const response = await result.current.mutateAsync(createTokenRequest);
        expect(response.tokenValue).toBe('dapi_abcd1234567890');
      });

      expect(mockCLIENT.post).toHaveBeenCalledWith('/api/2.1/unity-catalog/tokens', createTokenRequest);
    });

    test('validates TTL exceeds maximum', async () => {
      const { result } = renderHook(() => useCreateToken(), {
        wrapper: createWrapper(),
      });

      const createTokenRequest = {
        comment: 'Test token',
        lifetimeSeconds: 6000000, // > 60 days
      };

      await expect(result.current.mutateAsync(createTokenRequest)).rejects.toThrow('Token lifetime cannot exceed 60 days');
    });

    test('validates token format', async () => {
      const mockResponse = {
        data: {
          tokenId: 'tok_123',
          tokenValue: 'invalid_token', // Doesn't start with dapi_
          comment: 'Test token',
          createdAt: '2025-01-15T10:00:00Z',
          expiresAt: '2025-01-16T10:00:00Z',
        }
      };

      mockCLIENT.post.mockResolvedValueOnce(mockResponse);

      const { result } = renderHook(() => useCreateToken(), {
        wrapper: createWrapper(),
      });

      await expect(result.current.mutateAsync({ lifetimeSeconds: 3600 })).rejects.toThrow('Invalid token format received');
    });
  });

  describe('useListTokens', () => {
    test('lists tokens successfully', async () => {
      const mockResponse = {
        data: {
          tokens: [
            {
              tokenId: 'tok_123',
              comment: 'Test token',
              createdAt: '2025-01-15T10:00:00Z',
              expiresAt: '2025-01-16T10:00:00Z',
              status: 'ACTIVE',
            }
          ]
        }
      };

      mockCLIENT.get.mockResolvedValueOnce(mockResponse);

      const { result } = renderHook(() => useListTokens(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(result.current.data?.tokens).toHaveLength(1);
        expect(result.current.data?.tokens[0].tokenId).toBe('tok_123');
      });

      expect(mockCLIENT.get).toHaveBeenCalledWith('/api/2.1/unity-catalog/tokens');
    });
  });

  describe('useRevokeToken', () => {
    test('revokes token successfully', async () => {
      mockCLIENT.delete.mockResolvedValueOnce({});

      const { result } = renderHook(() => useRevokeToken(), {
        wrapper: createWrapper(),
      });

      await waitFor(async () => {
        await result.current.mutateAsync('tok_123');
      });

      expect(mockCLIENT.delete).toHaveBeenCalledWith('/api/2.1/unity-catalog/tokens/tok_123');
    });
  });
});
