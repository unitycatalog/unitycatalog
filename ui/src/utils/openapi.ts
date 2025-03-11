import axios from 'axios';
import type { AxiosInstance, AxiosRequestConfig } from 'axios';

/**
 * Represents the type of HTTP methods.
 */
export type HttpMethod =
  | 'get'
  | 'put'
  | 'post'
  | 'delete'
  | 'options'
  | 'head'
  | 'patch'
  | 'trace';

/**
 * Represents the type of HTTP success codes.
 */
export type HttpSuccessCode =
  | 200 // OK
  | 201 // CREATED
  | 202 // ACCEPTED
  | 203 // NON_AUTHORITATIVE_INFORMATION
  | 204 // NO_CONTENT
  | 205 // RESET_CONTENT
  | 206 // PARTIAL_CONTENT
  | 207 // MULTI_STATUS
  | 208 // ALREADY_REPORTED
  | 226; // IM_USED

/**
 * Represents the type of HTTP error codes.
 */
export type HttpErrorCode =
  | 400 // BAD_REQUEST
  | 401 // UNAUTHORIZED
  | 402 // PAYMENT_REQUIRED
  | 403 // FORBIDDEN
  | 404 // NOT_FOUND
  | 405 // METHOD_NOT_ALLOWED
  | 406 // NOT_ACCEPTABLE
  | 407 // PROXY_AUTHENTICATION_REQUIRED
  | 408 // REQUEST_TIMEOUT
  | 409 // CONFLICT
  | 410 // GONE
  | 411 // LENGTH_REQUIRED
  | 412 // PRECONDITION_FAILED
  | 413 // PAYLOAD_TOO_LARGE
  | 414 // URI_TOO_LONG
  | 415 // UNSUPPORTED_MEDIA_TYPE
  | 416 // RANGE_NOT_SATISFIABLE
  | 417 // EXPECTATION_FAILED
  | 418 // IM_A_TEAPOT
  | 421 // MISDIRECTED_REQUEST
  | 422 // UNPROCESSABLE_CONTENT
  | 423 // LOCKED
  | 424 // FAILED_DEPENDENCY
  | 425 // TOO_EARLY
  | 426 // UPGRADE_REQUIRED
  | 428 // PRECONDITION_REQUIRED
  | 429 // TOO_MANY_REQUESTS
  | 431 // REQUEST_HEADER_FIELDS_TOO_LARGE
  | 451 // UNAVAILABLE_FOR_LEGAL_REASONS
  | 500 // INTERNAL_SERVER_ERROR
  | 501 // NOT_IMPLEMENTED
  | 502 // BAD_GATEWAY
  | 503 // SERVICE_UNAVAILABLE
  | 504 // GATEWAY_TIMEOUT
  | 505 // HTTP_VERSION_NOT_SUPPORTED
  | 506 // VARIANT_ALSO_NEGOTIATES
  | 507 // INSUFFICIENT_STORAGE
  | 508 // LOOP_DETECTED
  | 510 // NOT_EXTENDED
  | 511; // NETWORK_AUTHENTICATION_REQUIRED

/**
 * Represents the type of application media-types (not exhaustive).
 *
 * See also:
 * - {@link https://www.iana.org/assignments/media-types/media-types.xhtml#application | Application Media Types }
 */
export type MediaType =
  | 'application/json' // JSON
  | 'application/x-www-form-urlencoded'; // URL Encoded Form

/**
 * Utility type that extracts the type specified by a given `Path` from `T`.
 *
 * Example:
 * - `Get<{ a: { b: { c: { someKey: someValue } } } }, ['a', 'b', 'c']>` results in `{ someKey: someValue }`.
 */
type Get<
  T extends Record<string | number | symbol, any>,
  Path extends (string | number | symbol)[],
> = 0 extends Path['length']
  ? T
  : Path extends [infer Head, ...infer Tail]
    ? Head extends keyof T
      ? Tail extends (string | number | symbol)[]
        ? Get<0 extends Tail['length'] ? T[Head] : Required<T[Head]>, Tail>
        : never
      : never
    : never;

/**
 * Represents the type of predefined API schemas.
 */
export type Spec = Record<string | number | symbol, any>;

/**
 * Represents the type of predefined API paths of a given `Api`.
 */
export type PathOf<Api extends Spec> = keyof Api;

/**
 * Utility type that extracts the HTTP status codes of given `Api`, `Path` and `Method`.
 */
type CodeOf<
  Api extends Spec,
  Path extends PathOf<Api>,
  Method extends HttpMethod,
> = keyof Get<Api, [Path, Method, 'responses']>;

/**
 * Utility type that extracts the JSON responses of given `Api`, `Path`, `Method` and `Code`.
 */
type ContentOf<
  Api extends Spec,
  Path extends PathOf<Api>,
  Method extends HttpMethod,
  Code extends HttpSuccessCode | HttpErrorCode,
  ContentType extends MediaType,
> = Get<Api, [Path, Method, 'responses', Code, 'content', ContentType]>;

/**
 * Represents the type of predefined API components of a given `Api`.
 */
type ComponentOf<Api extends Spec> = keyof Api['schemas'];

/**
 * Utility type that expands the HTTP error responses of given `Api`, `Path` and `Method` with a given `ErrorCode`.
 * If an error response is defined in the schema, its type is applied to `data`.
 * Otherwise, the type `{ message: string }` is applied to `data`.
 */
type ErrorWithStatus<
  Api extends Spec,
  Path extends PathOf<Api>,
  Method extends HttpMethod,
  ErrorCode extends HttpErrorCode,
  ContentType extends MediaType = 'application/json',
> =
  ContentOf<Api, Path, Method, ErrorCode, ContentType> extends never
    ? {
        status: ErrorCode;
        data: {
          message: string;
        };
      }
    : {
        status: ErrorCode;
        data: ContentOf<Api, Path, Method, ErrorCode, ContentType>;
      };

/**
 * Represents the type of predefined API path parameters of given `Api`, `Path` and `Method`.
 */
export type PathParam<
  Api extends Spec,
  Path extends PathOf<Api>,
  Method extends HttpMethod,
> = Get<Api, [Path, Method, 'parameters', 'path']>;

/**
 * Represents the type of predefined API query parameters of given `Api`, `Path` and `Method`.
 */
export type QueryParam<
  Api extends Spec,
  Path extends PathOf<Api>,
  Method extends HttpMethod,
> = Get<Api, [Path, Method, 'parameters', 'query']>;

/**
 * Represents the type of predefined API bodies of given `Api`, `Path` and `Method`.
 */
export type RequestBody<
  Api extends Spec,
  Path extends PathOf<Api>,
  Method extends HttpMethod,
  ContentType extends MediaType = 'application/json',
> = Get<Api, [Path, Method, 'requestBody', 'content', ContentType]>;

/**
 * Represents the type of predefined API success bodies of given `Api`, `Path` and `Method`.
 */
export type SuccessResponseBody<
  Api extends Spec,
  Path extends PathOf<Api>,
  Method extends HttpMethod,
  ContentType extends MediaType = 'application/json',
> = ContentOf<
  Api,
  Path,
  Method,
  CodeOf<Api, Path, Method> & HttpSuccessCode,
  ContentType
>;

/**
 * Represents the type of predefined API error bodies of given `Api`, `Path` and `Method`.
 */
export type ErrorResponseBody<
  Api extends Spec,
  Path extends PathOf<Api>,
  Method extends HttpMethod,
  ErrorCode extends HttpErrorCode,
  ContentType extends MediaType = 'application/json',
> = ErrorWithStatus<Api, Path, Method, ErrorCode, ContentType>;

/**
 * Represents the type of predefined API responses of given `Api`, `Path` and `Method`.
 */
export type SuccessResponse<
  Api extends Spec,
  Path extends PathOf<Api>,
  Method extends HttpMethod,
  ContentType extends MediaType = 'application/json',
> = {
  result: 'success';
  data: SuccessResponseBody<Api, Path, Method, ContentType>;
};

/**
 * Represents the type of predefined API responses of given `Api`, `Path`, `Method` and `ErrorCode`.
 */
export type ErrorResponse<
  Api extends Spec,
  Path extends PathOf<Api>,
  Method extends HttpMethod,
  ErrorCode extends HttpErrorCode,
  ContentType extends MediaType = 'application/json',
> = {
  result: 'error';
  data: ErrorResponseBody<Api, Path, Method, ErrorCode, ContentType>;
};

/**
 * Represents the type of the predefined API request of given `Api`, `Path` and `Method`.
 */
export type Request<
  Api extends Spec,
  Path extends PathOf<Api>,
  Method extends HttpMethod,
  ContentType extends MediaType = 'application/json',
> = {
  path: Path;
  method: Method;
  params?: {
    paths?: PathParam<Api, Path, Method>;
    query?: QueryParam<Api, Path, Method>;
    body?: RequestBody<Api, Path, Method, ContentType>;
  };
};

/**
 * Represents the type of predefined API responses of given `Api`, `Path`, `Method` and `ErrorCode`.
 */
export type Response<
  Api extends Spec,
  Path extends PathOf<Api>,
  Method extends HttpMethod,
  ErrorCode extends HttpErrorCode,
  ContentType extends MediaType = 'application/json',
> =
  | SuccessResponse<Api, Path, Method, ContentType>
  | ErrorResponse<Api, Path, Method, ErrorCode, ContentType>;

/**
 * Represents the type of predefined API components of given `Api` and `Component`.
 */
export type Model<Api extends Spec, Component extends ComponentOf<Api>> = Get<
  Api,
  ['schemas', Component]
>;

/**
 * Represents the configuration for a client request, which is a partial type based on `AxiosRequestConfig`.
 */
export type RequestConfig = Omit<
  AxiosRequestConfig,
  'url' | 'method' | 'params' | 'data'
>;

/**
 * Represents the arguments of `route` functions.
 */
export type RouteArgs<
  Api extends Spec,
  Path extends PathOf<Api>,
  Method extends HttpMethod,
  ErrorCode extends HttpErrorCode,
> = {
  client: AxiosInstance;
  request: Request<Api, Path, Method, MediaType>;
  config?: RequestConfig;
  errorMessage?: string;
  errorTypeGuard?: (response: {
    status: number;
    data: any;
  }) => response is ErrorResponseBody<Api, Path, Method, ErrorCode, MediaType>;
};

/**
 * Utility type that simulates partial type argument inference.
 *
 * See also:
 * - {@link https://github.com/microsoft/TypeScript/issues/26242 | Partial Type Argument Inference }
 */
export type Route<Api extends Spec> = {
  <
    Path extends PathOf<Api>,
    Method extends HttpMethod,
    ErrorCode extends HttpErrorCode = never,
  >(
    args: RouteArgs<Api, Path, Method, ErrorCode>,
  ): {
    path: () => string;
    call: () => Promise<Response<Api, Path, Method, ErrorCode, MediaType>>;
  };
};

/**
 * Type-guard for the `ErrorResponse`.
 */
export function isError<
  Api extends Spec,
  ErrorCode extends HttpErrorCode,
  ContentType extends MediaType = 'application/json',
>(
  response: Response<Api, PathOf<Api>, HttpMethod, ErrorCode, ContentType>,
): response is ErrorResponse<
  Api,
  PathOf<Api>,
  HttpMethod,
  ErrorCode,
  ContentType
> {
  return response.result === 'error';
}

/**
 * Ensures that the relevant statement is exhaustive.
 */
export function assertNever(value: never): never {
  throw new Error('Unexpected value: ' + value);
}

/**
 * Configures the API `client` using the specified `request` context.
 */
export function route<
  Api extends Spec,
  Path extends PathOf<Api>,
  Method extends HttpMethod,
  ErrorCode extends HttpErrorCode = never,
>({
  client,
  request,
  config = {},
  errorMessage = 'Unexpected error',
  errorTypeGuard = (response: {
    status: number;
    data: any;
  }): response is ErrorResponseBody<Api, Path, Method, ErrorCode, MediaType> =>
    false,
}: RouteArgs<Api, Path, Method, ErrorCode>) {
  // Converts a path like {key} into its actual value.
  const path = () =>
    Object.entries(request.params?.paths ?? {}).reduce(
      (acc, [key, value]) =>
        acc.replace(new RegExp(`\\{${key}\\}`), String(value)),
      request.path as string,
    );

  // Conducts the actual API call.
  const call = async (): Promise<
    Response<Api, Path, Method, ErrorCode, MediaType>
  > => {
    try {
      const response = await client.request<
        SuccessResponseBody<Api, Path, Method, MediaType>
      >({
        url: path(),
        method: request.method,
        params: request.params?.query,
        data: request.params?.body,
        ...config,
      });
      return {
        result: 'success',
        data: response.data,
      };
    } catch (e: any) {
      if (axios.isAxiosError(e) && !!e.response) {
        const error = { status: e.response.status, data: e.response.data };
        if (errorTypeGuard(error)) {
          return {
            result: 'error',
            data: error,
          };
        }
      }
      throw new Error(e.response?.data?.message || errorMessage);
    }
  };

  return { path, call };
}
