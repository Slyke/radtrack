export interface AuthUser {
  id: string;
  username: string;
  role: 'view_only' | 'standard' | 'moderator' | 'admin';
  mustChangePassword: boolean;
  isDisabled: boolean;
}

export interface UiConfig {
  theme: string;
  font: string;
  language: string;
  tileUrlTemplate: string;
  attribution: string;
  defaultMetric: string;
  defaultAggregateShape: string;
  defaultCellSizeMeters: number;
  rawPointCap: number;
  modeBucketDecimals: number;
  cacheTtlSeconds: number;
}

export interface BuildInfo {
  version: string;
  commitHash: string;
  label: string;
}

export interface SessionPayload {
  authenticated: boolean;
  user: AuthUser | null;
  authMethod: string | null;
  csrf: {
    headerName: string;
    token: string;
  } | null;
  authModes: {
    local: boolean;
    oidc: boolean;
    header: boolean;
  };
  ui: UiConfig | null;
  build: BuildInfo | null;
}
