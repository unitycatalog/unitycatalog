export interface CreateTokenRequest {
  comment: string; // Required field
  lifetimeSeconds?: number;
}

export interface CreateTokenResponse {
  tokenId: string;
  tokenValue: string; // Prefixed with "dapi_", displayed once only
  comment?: string;
  createdAt: string;
  expiresAt: string;
}

export interface TokenInfo {
  tokenId: string;
  comment?: string;
  createdAt: string;
  expiresAt: string;
  lastUsedAt?: string;
  status: 'ACTIVE' | 'EXPIRED' | 'REVOKED';
  // tokenValue is NEVER returned in list responses for security
}

export interface ListTokensResponse {
  tokens: TokenInfo[];
}

export const TTL_OPTIONS = [
  { label: '1 hour', value: 3600, seconds: 3600 },
  { label: '24 hours', value: 86400, seconds: 86400 },
  { label: '7 days', value: 604800, seconds: 604800 },
  { label: '30 days', value: 2592000, seconds: 2592000 },
  { label: '60 days (max)', value: 5184000, seconds: 5184000 }
] as const;

export const MAX_TTL_SECONDS = 5184000; // 60 days
