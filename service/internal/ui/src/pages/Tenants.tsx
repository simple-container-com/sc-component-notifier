import { useQuery } from '@tanstack/react-query'
import { api, describeError } from '../api/client'

export function Tenants() {
  // Authenticated by the operator session cookie — App only renders this
  // once the session probe says so. There is no token handling here on
  // purpose: the first deploy asked the operator to paste a hand-minted
  // JWT into `localStorage.notifierToken`, which was both unusable and
  // the wrong secret to put in a browser.
  const { data, isLoading, error } = useQuery({
    queryKey: ['tenants'],
    queryFn: api.tenants,
    refetchInterval: 5000,
  })

  if (isLoading) return <p>Loading…</p>
  if (error) {
    return <p style={{ color: '#c00' }}>Error fetching tenants: {describeError(error)}</p>
  }

  const tenants = data?.tenants ?? []
  if (tenants.length === 0) {
    return (
      <div>
        <h2>Tenants</h2>
        <p style={{ color: '#666' }}>No tenants registered yet.</p>
        <p style={{ fontSize: '0.9em', color: '#888' }}>
          A tenant appears when a consumer service deploys with <code>uses: [my-notifier]</code> in
          its <code>client.yaml</code>. The plugin's <code>ComputeProcess</code> handler pushes the
          registration to <code>POST /api/v1/tenants/register</code>.
        </p>
      </div>
    )
  }

  return (
    <div>
      <h2>Tenants ({data!.count})</h2>
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr style={{ borderBottom: '2px solid #333' }}>
            <th style={{ textAlign: 'left', padding: '0.5rem' }}>Stack</th>
            <th style={{ textAlign: 'left', padding: '0.5rem' }}>Version</th>
            <th style={{ textAlign: 'left', padding: '0.5rem' }}>Resources</th>
            <th style={{ textAlign: 'left', padding: '0.5rem' }}>Last seen</th>
            <th style={{ textAlign: 'left', padding: '0.5rem' }}>State</th>
          </tr>
        </thead>
        <tbody>
          {tenants.map((t) => (
            <tr key={t.stackName} style={{ borderBottom: '1px solid #eee' }}>
              <td style={{ padding: '0.5rem', fontFamily: 'monospace' }}>{t.stackName}</td>
              <td style={{ padding: '0.5rem' }}>{t.version}</td>
              <td style={{ padding: '0.5rem' }}>{t.resources?.length ?? 0}</td>
              <td style={{ padding: '0.5rem', fontFamily: 'monospace', fontSize: '0.9em' }}>
                {new Date(t.lastSeenAt).toISOString()}
              </td>
              <td style={{ padding: '0.5rem' }}>{t.softDeleted ? 'deleted' : 'active'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
