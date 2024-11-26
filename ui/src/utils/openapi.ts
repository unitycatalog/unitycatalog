import axios from 'axios';
import type { AxiosInstance } from 'axios';

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
  | 200
  | 201
  | 202
  | 203
  | 204
  | 205
  | 206
  | 207
  | 208
  | 226;

/**
 * Represents the type of HTTP error codes.
 */
export type HttpErrorCode =
  | 400
  | 401
  | 402
  | 403
  | 404
  | 405
  | 406
  | 407
  | 408
  | 409
  | 410
  | 411
  | 412
  | 413
  | 414
  | 415
  | 416
  | 417
  | 418
  | 421
  | 422
  | 423
  | 424
  | 425
  | 426
  | 428
  | 429
  | 431
  | 451
  | 500
  | 501
  | 502
  | 503
  | 504
  | 505
  | 506
  | 507
  | 508
  | 510
  | 511;

/**
 * Utility type that converts a union (A | B | C) into an intersection ( A & B & C ).
 *
 * See also:
 * - {@link https://www.typescriptlang.org/docs/handbook/2/conditional-types.html#distributive-conditional-types | Distributive Conditional Types }
 * - {@link https://www.typescriptlang.org/docs/handbook/2/conditional-types.html#inferring-within-conditional-types | Inferring Within Conditional Types }
 */
type UnionToIntersection<T> = (T extends any ? (k: T) => void : never) extends (
  k: infer U,
) => void
  ? U
  : never;

/**
 * Utility type that extracts the type specified by a given `Path` from `T`.
 *
 * Example:
 * - `Get<{ a: { b: { c: { someKey: someValue } } } }, ['a', 'b', 'c']>` results in `{ someKey: someValue }`.
 */
type Get<
  T extends Record<string, any>,
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
export type Specification = Record<string | number | symbol, any>;

/**
 * Represents the type of predefined API paths of a given `Api`.
 */
export type ApiPath<Api extends Specification> = keyof Api;

/**
 * Represents the type of predefined HTTP methods of given `Api` and `Path`.
 */
export type ApiMethodOf<
  Api extends Specification,
  Path extends ApiPath<Api>,
> = HttpMethod & keyof UnionToIntersection<Api[Path]>;

/**
 * Represents the type of predefined API paths of given `Api` and `Method`.
 *
 * See also:
 * - {@link https://www.typescriptlang.org/docs/handbook/2/mapped-types.html#key-remapping-via-as | Key Remapping via as }
 */
export type ApiPathOf<
  Api extends Specification,
  Method extends HttpMethod,
> = Method extends any
  ? keyof {
      [K in keyof Api as Api[K] extends Record<Method, any>
        ? K
        : never]: Api[K];
    }
  : never;

/**
 * Utility type that extracts the HTTP status codes of given `Api`, `Path` and `Method`.
 */
type GetCode<
  Api extends Specification,
  Path extends ApiPath<Api>,
  Method extends HttpMethod,
> = keyof Get<Api, [Path, Method, 'responses']>;

/**
 * Utility type that extracts the JSON responses of given `Api`, `Path`, `Method` and `Code`.
 */
type GetContent<
  Api extends Specification,
  Path extends ApiPath<Api>,
  Method extends HttpMethod,
  Code extends number,
> = Get<Api, [Path, Method, 'responses', Code, 'content', 'application/json']>;

/**
 * Utility type that expands the HTTP error responses of given `Api`, `Path` and `Method` with a given `ErrorCode`.
 * If an error response is defined in the schema, its type is applied to `data`.
 * Otherwise, the type `{ message: string }` is applied to `data`.
 */
type ExpandApiError<
  Api extends Specification,
  Path extends ApiPath<Api>,
  Method extends HttpMethod,
  ErrorCode extends HttpErrorCode,
> = ErrorCode extends number
  ? GetContent<Api, Path, Method, ErrorCode> extends never
    ? {
        status: ErrorCode;
        data: {
          message: string;
        };
      }
    : {
        status: ErrorCode;
        data: GetContent<Api, Path, Method, ErrorCode>;
      }
  : never;

/**
 * Represents the type of predefined API path parameters of given `Api`, `Path` and `Method`.
 */
export type ApiRequestPathParam<
  Api extends Specification,
  Path extends ApiPath<Api>,
  Method extends HttpMethod,
> = Get<Api, [Path, Method, 'parameters', 'path']>;

/**
 * Represents the type of predefined API query parameters of given `Api`, `Path` and `Method`.
 */
export type ApiRequestQueryParam<
  Api extends Specification,
  Path extends ApiPath<Api>,
  Method extends HttpMethod,
> = Get<Api, [Path, Method, 'parameters', 'query']>;

/**
 * Represents the type of predefined API bodies of given `Api`, `Path` and `Method`.
 */
export type ApiRequestBody<
  Api extends Specification,
  Path extends ApiPath<Api>,
  Method extends HttpMethod,
> = Get<Api, [Path, Method, 'requestBody', 'content', 'application/json']>;

/**
 * Represents the type of predefined API responses of given `Api`, `Path` and `Method`.
 */
export type ApiSuccessResponse<
  Api extends Specification,
  Path extends ApiPath<Api>,
  Method extends HttpMethod,
> = GetContent<Api, Path, Method, GetCode<Api, Path, Method> & HttpSuccessCode>;

/**
 * Represents the type of predefined API errors of given `Api`, `Path` and `Method`.
 */
export type ApiErrorResponse<
  Api extends Specification,
  Path extends ApiPath<Api>,
  Method extends HttpMethod,
  ErrorCode extends HttpErrorCode,
> = ExpandApiError<Api, Path, Method, ErrorCode>;

/**
 * Represents the type of the predefined API request of given `Api`, `Path` and `Method`.
 */
export type ApiRequest<
  Api extends Specification,
  Path extends ApiPath<Api>,
  Method extends HttpMethod,
> = {
  path: Path;
  method: Method;
  params?: {
    paths?: ApiRequestPathParam<Api, Path, Method>;
    query?: ApiRequestQueryParam<Api, Path, Method>;
    body?: ApiRequestBody<Api, Path, Method>;
  };
};

/**
 * Represents the type of predefined API responses of given `Api`, `Path`, `Method` and `ErrorCode`.
 */
export type ApiResponse<
  Api extends Specification,
  Path extends ApiPath<Api>,
  Method extends HttpMethod,
  ErrorCode extends HttpErrorCode,
> =
  | { result: 'success'; data: ApiSuccessResponse<Api, Path, Method> }
  | {
      result: 'error';
      data: ApiErrorResponse<Api, Path, Method, ErrorCode>;
    };

/**
 * Represents the type of predefined API components of a given `Api`.
 */
export type ApiComponent<Api extends Specification> = keyof Api['schemas'];

/**
 * Represents the type of predefined API components of given `Api` and `Component`.
 */
export type ApiInterface<
  Api extends Specification,
  Component extends ApiComponent<Api>,
> = Get<Api, ['schemas', Component]>;

/**
 * Configures the API `client` using the specified `request` context.
 */
export const route = <
  Api extends Specification,
  Path extends ApiPath<Api>,
  Method extends HttpMethod,
  ErrorCode extends HttpErrorCode,
>(
  client: AxiosInstance,
  request: ApiRequest<Api, Path, Method>,
  isExpectedError: (response: {
    status: number;
    data: any;
  }) => response is ApiErrorResponse<
    Api,
    Path,
    Method,
    ErrorCode
  > = (response: {
    status: number;
    data: any;
  }): response is ApiErrorResponse<Api, Path, Method, ErrorCode> => false,
) => {
  // Converts a path like {key} into its actual value.
  const path = () => {
    const fullPath = Object.entries(request.params?.paths ?? {}).reduce(
      (prev, [key, value]) =>
        prev.replace(new RegExp(`\\{${key}\\}`), String(value)),
      request.path as string,
    );
    const searchParam = new URLSearchParams();
    Object.entries(request.params?.query ?? {}).forEach(([key, value]) => {
      if (typeof value === 'string' || typeof value === 'number') {
        searchParam.set(key, value.toString());
      }
    });
    if (searchParam.toString().length > 0) {
      return fullPath + '?' + searchParam.toString();
    }
    return fullPath;
  };

  // Conducts the actual API call.
  const call = async (): Promise<ApiResponse<Api, Path, Method, ErrorCode>> => {
    try {
      const response = await client.request<
        ApiSuccessResponse<Api, Path, Method>
      >({
        url: path(),
        method: request.method,
        data: request.params?.body,
        withCredentials: true,
      });
      return {
        result: 'success',
        data: response.data,
      };
    } catch (e) {
      if (axios.isAxiosError(e) && !!e.response) {
        const error = { status: e.response.status, data: e.response.data };
        if (isExpectedError(error)) {
          return {
            result: 'error',
            data: error,
          };
        }
      }
      throw new Error('Unexpected error');
    }
  };

  return { path, call };
};
