import { useQuery } from '@tanstack/react-query';
import { useAuth } from '../context/auth-context';
import { CLIENT } from '../context/client';
import { route, isError } from '../utils/openapi';
import type { paths as ControlApi } from '../types/api/control.gen';
import type { paths as CatalogApi, components as CatalogComponent } from '../types/api/catalog.gen';
import type { Route } from '../utils/openapi';
import { UC_AUTH_API_PREFIX, UC_API_PREFIX } from '../utils/constants';

interface AdminStatus {
  isAdmin: boolean;
  canManageUsers: boolean;
  canManagePermissions: boolean;
  userTestResponse?: any;
  permissionTestResponse?: any;
}

export const useAdminStatus = () => {
  const { currentUser } = useAuth();
  
  return useQuery<AdminStatus>({
    queryKey: ['admin-status', currentUser?.id],
    queryFn: async (): Promise<AdminStatus> => {
      if (!currentUser) {
        return { 
          isAdmin: false, 
          canManageUsers: false, 
          canManagePermissions: false,
          userTestResponse: 'No current user',
          permissionTestResponse: 'No current user'
        };
      }
      
      let canManageUsers = false;
      let canManagePermissions = false;
      let userTestResponse = null;
      let permissionTestResponse = null;
      
      // Test user management access using the same pattern as admin hooks
      try {
        const usersResponse = await (route as Route<ControlApi>)({
          client: CLIENT,
          request: {
            path: '/scim2/Users',
            method: 'get',
          },
          config: {
            baseURL: UC_AUTH_API_PREFIX,
          },
          errorMessage: 'Failed to test user access',
        }).call();

        if (!isError(usersResponse)) {
          canManageUsers = true;
          userTestResponse = 'Success - can list users';
        } else {
          userTestResponse = `Error: Failed to list users`;
        }
      } catch (error: any) {
        userTestResponse = `Exception: ${error.message}`;
      }
      
      // Test permissions access
      try {
        const permissionsResponse = await (route as Route<CatalogApi>)({
          client: CLIENT,
          request: {
            path: '/permissions/{securable_type}/{full_name}',
            method: 'get',
            params: {
              paths: {
                securable_type: 'metastore' as any,
                full_name: 'unity',
              },
            },
          },
          config: {
            baseURL: UC_API_PREFIX,
          },
          errorMessage: 'Failed to test permissions access',
        }).call();

        if (!isError(permissionsResponse)) {
          canManagePermissions = true;
          permissionTestResponse = 'Success - can query permissions';
        } else {
          permissionTestResponse = `Error: Failed to query permissions`;
        }
      } catch (error: any) {
        permissionTestResponse = `Exception: ${error.message}`;
      }
      
      const isAdmin = canManageUsers || canManagePermissions;
      
      return { 
        isAdmin, 
        canManageUsers, 
        canManagePermissions,
        userTestResponse,
        permissionTestResponse
      };
    },
    enabled: !!currentUser,
    retry: false,
    staleTime: 2 * 60 * 1000, // 2 minutes
  });
};
