import { useMutation, useQuery } from '@tanstack/react-query';
import { CLIENT } from '../context/client';
import { TokenEndpointExtensionType } from '../types/api/control.gen';
import { UC_AUTH_API_PREFIX } from '../utils/constants';
import { route, isError, assertNever } from '../utils/openapi';
import type {
  paths as ControlApi,
  components as ControlComponent,
} from '../types/api/control.gen';
import type {
  Model,
  RequestBody,
  ErrorResponseBody,
  Route,
  SuccessResponseBody,
} from '../utils/openapi';

export interface OAuthTokenExchangeInterface
  extends Model<ControlComponent, 'OAuthTokenExchangeInfo'> {}

export interface LoginWithTokenMutationParams
  extends RequestBody<
    ControlApi,
    '/auth/tokens',
    'post',
    'application/x-www-form-urlencoded'
  > {}

export function useLoginWithToken() {
  return useMutation<
    OAuthTokenExchangeInterface,
    Error,
    LoginWithTokenMutationParams
  >({
    mutationFn: async (params: LoginWithTokenMutationParams) => {
      const response = await (route as Route<ControlApi>)({
        client: CLIENT,
        request: {
          path: '/auth/tokens',
          method: 'post',
          params: {
            query: {
              ext: TokenEndpointExtensionType.cookie,
            },
            body: params,
          },
        },
        config: {
          baseURL: UC_AUTH_API_PREFIX,
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        },
        errorMessage: 'Failed to login',
      }).call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        return assertNever(response.data.status);
      } else {
        return response.data;
      }
    },
  });
}

export interface LogoutCurrentUserMutationParams {}

export function useLogoutCurrentUser() {
  return useMutation<
    SuccessResponseBody<ControlApi, '/auth/logout', 'post'>,
    Error,
    LogoutCurrentUserMutationParams
  >({
    mutationFn: async () => {
      const response = await (route as Route<ControlApi>)({
        client: CLIENT,
        request: {
          path: '/auth/logout',
          method: 'post',
        },
        config: {
          baseURL: UC_AUTH_API_PREFIX,
        },
        errorMessage: 'Failed to logout',
      }).call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        return assertNever(response.data.status);
      } else {
        return response.data;
      }
    },
  });
}

export interface UserInterface
  extends Model<ControlComponent, 'UserResource'> {}

export function useGetCurrentUser() {
  const expectedErrorCodes = [
    401, // UNAUTHORIZED
  ] as const;

  type ErrorCode = (typeof expectedErrorCodes)[number];

  const isExpectedError = (response: {
    status: number;
    data: any;
  }): response is ErrorResponseBody<
    ControlApi,
    '/scim2/Me',
    'get',
    ErrorCode
  > => expectedErrorCodes.map(Number).includes(response.status);

  return useQuery<SuccessResponseBody<ControlApi, '/scim2/Me', 'get'> | null>({
    queryKey: ['getUser'],
    queryFn: async () => {
      const response = await (route as Route<ControlApi>)({
        client: CLIENT,
        request: {
          path: '/scim2/Me',
          method: 'get',
        },
        config: {
          baseURL: UC_AUTH_API_PREFIX,
        },
        errorMessage: 'Failed to fetch user',
        errorTypeGuard: isExpectedError,
      }).call();
      if (isError(response)) {
        switch (response.data.status) {
          case 401:
            return null;
          default:
            return assertNever(response.data.status);
        }
      } else {
        return response.data;
      }
    },
  });
}
