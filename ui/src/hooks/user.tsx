import { useMutation, useQuery } from '@tanstack/react-query';
import apiClient from '../context/client';
import { UC_AUTH_API_PREFIX } from '../utils/constants';

interface LoginResponse {
  access_token: string;
}

export interface UserInterface {
  id: string;
  userName: string;
  displayName: string;
  emails: any;
  photos: any;
}

export function useLoginWithToken() {
  return useMutation<LoginResponse, Error, string>({
    mutationFn: async (idToken) => {
      const params = {
        grantType: 'urn:ietf:params:oauth:grant-type:token-exchange',
        requestedTokenType: 'urn:ietf:params:oauth:token-type:access_token',
        subjectTokenType: 'urn:ietf:params:oauth:token-type:id_token',
        subjectToken: idToken,
      };

      return apiClient
        .post(`/auth/tokens?ext=cookie`, JSON.stringify(params), {
          baseURL: `${UC_AUTH_API_PREFIX}`,
        })
        .then((response) => response.data)
        .catch((e) => {
          throw new Error(e.response?.data?.message || 'Failed to log in');
        });
    },
  });
}

export function useGetCurrentUser() {
  return useQuery<UserInterface>({
    queryKey: ['getUser'],
    queryFn: async () => {
      return apiClient
        .get(`/scim2/Users/self`, {
          baseURL: `${UC_AUTH_API_PREFIX}`,
        })
        .then((response) => response.data)
        .catch(() => {
          throw new Error('Failed to fetch user');
        });
    },
  });
}
