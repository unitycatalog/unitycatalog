import { useMutation, useQuery } from '@tanstack/react-query';
import { CLIENT } from '../context/client';
import { route, isError, assertNever } from '../utils/openapi';
import type {
  paths as ControlApi,
  components as ControlComponent,
} from '../types/api/control.gen';
import type {
  paths as CatalogApi,
  components as CatalogComponent,
} from '../types/api/catalog.gen';
import type {
  Route,
  SuccessResponseBody,
  ErrorResponseBody,
  RequestBody,
} from '../utils/openapi';
import { UC_AUTH_API_PREFIX, UC_API_PREFIX } from '../utils/constants';
import { useAuth } from '../context/auth-context';
import { useMsalAuth } from '../context/msal-auth-context';

// TODO: Replace with actual admin token from context when available
// Currently using Unity Catalog token from auth context
function useAdminHeaders() {
  const { currentUser } = useAuth();
  const { getIdToken } = useMsalAuth();
  
  // TODO: Implement proper admin token retrieval
  // For now, we'll assume the current user's token has admin privileges
  // This should be replaced with actual admin token detection logic
  return {
    // Authorization header will be automatically added by axios interceptor
    // when we have proper admin token management
  };
}

// Users API
export function useListUsers() {
  const adminHeaders = useAdminHeaders();
  
  return useQuery({
    queryKey: ['admin', 'users'],
    queryFn: async () => {
      try {
        const response = await (route as Route<ControlApi>)({
          client: CLIENT,
          request: {
            path: '/scim2/Users',
            method: 'get',
          },
          config: {
            baseURL: UC_AUTH_API_PREFIX,
            headers: adminHeaders,
          },
          errorMessage: 'Failed to fetch users',
        }).call();

        if (isError(response)) {
          throw new Error('Failed to fetch users');
        } else {
          return (response.data as any)?.Resources || [];
        }
      } catch (error) {
        console.error('Admin users fetch error:', error);
        throw error;
      }
    },
  });
}

export function useCreateUser() {
  const adminHeaders = useAdminHeaders();

  return useMutation({
    mutationFn: async (userData: any) => {
      try {
        const response = await (route as Route<ControlApi>)({
          client: CLIENT,
          request: {
            path: '/scim2/Users',
            method: 'post',
            params: {
              body: userData,
            },
          },
          config: {
            baseURL: UC_AUTH_API_PREFIX,
            headers: adminHeaders,
          },
          errorMessage: 'Failed to create user',
        }).call();

        if (isError(response)) {
          throw new Error('Failed to create user');
        } else {
          return response.data;
        }
      } catch (error) {
        console.error('Admin user create error:', error);
        throw error;
      }
    },
  });
}

export function useUpdateUser() {
  const adminHeaders = useAdminHeaders();

  return useMutation({
    mutationFn: async ({ id, user }: { id: string; user: any }) => {
      try {
        const response = await (route as Route<ControlApi>)({
          client: CLIENT,
          request: {
            path: '/scim2/Users/{id}',
            method: 'put',
            params: {
              paths: { id },
              body: user,
            },
          },
          config: {
            baseURL: UC_AUTH_API_PREFIX,
            headers: adminHeaders,
          },
          errorMessage: 'Failed to update user',
        }).call();

        if (isError(response)) {
          throw new Error('Failed to update user');
        } else {
          return response.data;
        }
      } catch (error) {
        console.error('Admin user update error:', error);
        throw error;
      }
    },
  });
}

export function useDeleteUser() {
  const adminHeaders = useAdminHeaders();

  return useMutation({
    mutationFn: async (userId: string) => {
      try {
        const response = await (route as Route<ControlApi>)({
          client: CLIENT,
          request: {
            path: '/scim2/Users/{id}',
            method: 'delete',
            params: {
              paths: { id: userId },
            },
          },
          config: {
            baseURL: UC_AUTH_API_PREFIX,
            headers: adminHeaders,
          },
          errorMessage: 'Failed to delete user',
        }).call();

        if (isError(response)) {
          throw new Error('Failed to delete user');
        } else {
          return response.data;
        }
      } catch (error) {
        console.error('Admin user delete error:', error);
        throw error;
      }
    },
  });
}

// Permissions API
export function useGetPermissions(params: {
  resourceType?: CatalogComponent['schemas']['SecurableType'];
  resourceName?: string;
  principal?: string;
}) {
  const adminHeaders = useAdminHeaders();
  
  return useQuery({
    queryKey: ['admin', 'permissions', params],
    queryFn: async () => {
      if (!params.resourceType || !params.resourceName) {
        return null;
      }

      try {
        const response = await (route as Route<CatalogApi>)({
          client: CLIENT,
          request: {
            path: '/permissions/{securable_type}/{full_name}',
            method: 'get',
            params: {
              paths: {
                securable_type: params.resourceType,
                full_name: params.resourceName,
              },
              query: params.principal ? { principal: params.principal } : {},
            },
          },
          config: {
            baseURL: UC_API_PREFIX,
            headers: adminHeaders,
          },
          errorMessage: 'Failed to fetch permissions',
        }).call();

        if (isError(response)) {
          throw new Error('Failed to fetch permissions');
        } else {
          return response.data;
        }
      } catch (error) {
        console.error('Admin permissions fetch error:', error);
        throw error;
      }
    },
    enabled: Boolean(params.resourceType && params.resourceName),
  });
}

export function useUpdatePermissions() {
  const adminHeaders = useAdminHeaders();

  return useMutation({
    mutationFn: async (params: {
      resourceType: CatalogComponent['schemas']['SecurableType'];
      resourceName: string;
      changes: CatalogComponent['schemas']['PermissionsChange'][];
    }) => {
      try {
        const response = await (route as Route<CatalogApi>)({
          client: CLIENT,
          request: {
            path: '/permissions/{securable_type}/{full_name}',
            method: 'patch',
            params: {
              paths: {
                securable_type: params.resourceType,
                full_name: params.resourceName,
              },
              body: {
                changes: params.changes,
              },
            },
          },
          config: {
            baseURL: UC_API_PREFIX,
            headers: adminHeaders,
          },
          errorMessage: 'Failed to update permissions',
        }).call();

        if (isError(response)) {
          throw new Error('Failed to update permissions');
        } else {
          return response.data;
        }
      } catch (error) {
        console.error('Admin permissions update error:', error);
        throw error;
      }
    },
  });
}
