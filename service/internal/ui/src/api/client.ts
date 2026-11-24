import axios from 'axios'

// One axios instance for the whole SPA, so the credential policy is
// stated once.
//
// `withCredentials` is strictly speaking redundant: the SPA is served by
// the same Lambda that serves /api, and a browser sends same-origin
// cookies regardless. It is set anyway because the day someone runs the
// dev server against a deployed backend, the omission presents as a flat
// 401 with nothing pointing at the cause.
export const http = axios.create({ withCredentials: true })

export interface SessionInfo {
  authenticated: boolean
  // password_set false means this deployment has NO operator password
  // configured, which is a different problem from a wrong one — see the
  // Go side's SessionConfig.Session.
  password_set: boolean
  // The build serving this page, CalVer `vYYYY.MM.DD-<short-sha>`.
  // Optional because an older deployed build does not send it — not
  // because it is conditional on auth.
  version?: string
}

export interface TenantRecord {
  stackName: string
  version: string
  resources?: Array<{ type: string; name: string }>
  createdAt: string
  lastSeenAt: string
  softDeleted?: boolean
}

export interface TenantsResponse {
  tenants: TenantRecord[]
  count: number
}

export const api = {
  session: async (): Promise<SessionInfo> => (await http.get<SessionInfo>('/api/v1/session')).data,

  // 204 on success; the session cookie rides back in Set-Cookie and the
  // page never sees the token. That is the point: it is signed with the
  // same key that seals every tenant's channel credentials, so it must
  // not be reachable from JavaScript.
  login: async (password: string): Promise<void> => {
    await http.post('/api/v1/login', { password })
  },

  logout: async (): Promise<void> => {
    await http.post('/api/v1/logout')
  },

  tenants: async (): Promise<TenantsResponse> =>
    (await http.get<TenantsResponse>('/api/v1/tenants')).data,
}

// describeError turns an axios failure into something an operator can
// act on. A bare "Request failed with status code 401" names the symptom
// and hides the cause.
export function describeError(e: unknown): string {
  if (axios.isAxiosError(e)) {
    const status = e.response?.status
    const body = e.response?.data as { error?: string } | undefined
    if (body?.error) return body.error
    if (status === 401) return 'Your session expired. Sign in again.'
    if (status === 403) return 'This session is not an operator session.'
    return e.message
  }
  return e instanceof Error ? e.message : String(e)
}
