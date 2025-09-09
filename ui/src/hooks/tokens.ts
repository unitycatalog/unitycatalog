import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { CLIENT } from '../context/client';
import { CreateTokenRequest, CreateTokenResponse, ListTokensResponse } from '../types/tokens';
import { useNotification } from '../utils/NotificationContext';

export function useCreateToken() {
  const queryClient = useQueryClient();
  const { setNotification } = useNotification();

  return useMutation({
    mutationFn: async (request: CreateTokenRequest): Promise<CreateTokenResponse> => {
      // Client-side TTL validation
      if (request.lifetimeSeconds && request.lifetimeSeconds > 5184000) {
        throw new Error('Token lifetime cannot exceed 60 days (5184000 seconds)');
      }

      const response = await CLIENT.post('/tokens', request);
      
      if (!response.data.tokenValue?.startsWith('dapi_')) {
        throw new Error('Invalid token format received');
      }
      
      return response.data;
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['developer-tokens'] });
      setNotification(`Token "${data.comment || 'Unnamed'}" created successfully`, 'success');
    },
    onError: (error: any) => {
      const message = error.response?.data?.message || error.message || 'Failed to create token';
      setNotification(`Token creation failed: ${message}`, 'error');
    },
  });
}

export function useListTokens() {
  return useQuery({
    queryKey: ['developer-tokens'],
    queryFn: async (): Promise<ListTokensResponse> => {
      const response = await CLIENT.get('/tokens');
      return response.data;
    },
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
}

export function useRevokeToken() {
  const queryClient = useQueryClient();
  const { setNotification } = useNotification();

  return useMutation({
    mutationFn: async (tokenId: string): Promise<void> => {
      await CLIENT.delete(`/tokens/${tokenId}`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['developer-tokens'] });
      setNotification('Token revoked successfully', 'success');
    },
    onError: (error: any) => {
      const message = error.response?.data?.message || 'Failed to revoke token';
      setNotification(`Token revocation failed: ${message}`, 'error');
    },
  });
}
