import { useMutation, useQuery } from '@tanstack/react-query';
import { CLIENT } from '../context/control';
import { route, isError, assertNever } from '../utils/openapi';
import type {
  paths as ControlApi,
  components as ControlComponent,
} from '../types/api/control.gen';
import type {
  Model,
  ErrorResponseBody,
  SuccessResponseBody,
} from '../utils/openapi';

// TODO:
// As of [28/11/2024], the OpenAPI specification for auth-related APIs has not been defined.
// Once the specification is added, the following hooks should be updated.
// SEE:
// https://github.com/unitycatalog/unitycatalog/issues/768
interface LoginResponse {
  access_token: string;
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

      return CLIENT.post(`/auth/tokens?ext=cookie`, JSON.stringify(params))
        .then((response) => response.data)
        .catch((e) => {
          throw new Error(e.response?.data?.message || 'Failed to log in');
        });
    },
  });
}

// TODO:
// As of [28/11/2024], the OpenAPI specification for auth-related APIs has not been defined.
// Once the specification is added, the following hooks should be updated.
// SEE:
// https://github.com/unitycatalog/unitycatalog/issues/768
enum HttpStatus {
  OK = 200,
  CREATED = 201,
  BAD_REQUEST = 400,
  UNAUTHORIZED = 401,
  NOT_FOUND = 404,
  INTERNAL_SERVER_ERROR = 500,
}

interface LogoutResponse {
  response: HttpStatus;
}

export function useLogoutCurrentUser() {
  return useMutation<LogoutResponse, Error, {}>({
    mutationFn: async () => {
      return CLIENT.post(`/auth/logout`, {})
        .then((response) => response.data)
        .catch((e) => {
          throw new Error(e.response?.data?.message || 'Logout method failed');
        });
    },
  });
}

export type UserInterface = Model<ControlComponent, 'UserResource'>;

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
      const api = route({
        client: CLIENT,
        request: {
          path: '/scim2/Me',
          method: 'get',
        },
        errorMessage: 'Failed to fetch user',
        errorTypeGuard: isExpectedError,
      });
      const response = await api.call();
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
