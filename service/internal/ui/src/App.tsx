import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, describeError } from './api/client'
import { Login } from './pages/Login'
import { Tenants } from './pages/Tenants'

export function App() {
  // The probe is unauthenticated on the Go side precisely so this can
  // run before any cookie exists.
  const session = useQuery({ queryKey: ['session'], queryFn: api.session, retry: false })
  const qc = useQueryClient()
  const logout = useMutation({
    mutationFn: api.logout,
    onSuccess: () => void qc.invalidateQueries(),
  })

  if (session.isLoading) return <p style={{ textAlign: 'center', marginTop: '4rem' }}>Starting…</p>
  if (session.error) {
    return (
      <p style={{ color: '#c00', textAlign: 'center', marginTop: '4rem' }}>
        {describeError(session.error)}
      </p>
    )
  }
  if (!session.data?.authenticated) return <Login session={session.data!} />

  return (
    <main
      style={{
        fontFamily: 'system-ui, sans-serif',
        maxWidth: 900,
        margin: '2rem auto',
        padding: '0 1rem',
      }}
    >
      <header
        style={{
          borderBottom: '1px solid #ddd',
          paddingBottom: '0.5rem',
          marginBottom: '1.5rem',
          display: 'flex',
          alignItems: 'baseline',
          gap: '0.75rem',
        }}
      >
        <div>
          <h1 style={{ margin: 0 }}>Notifier</h1>
          <p style={{ color: '#666', margin: '0.25rem 0 0' }}>
            Routes failure events from SC components — backup runs, service health — to Telegram and
            webhooks.
          </p>
        </div>
        {/*
          The build serving this page. Small and grey on purpose — it is
          not information an operator needs while working, but it tells
          them at a glance whether the page they are looking at came
          from the build they just merged. A scripted smoke reads the
          same value off /healthz instead.
        */}
        {session.data.version && (
          <code
            style={{ marginLeft: 'auto', color: '#999', fontSize: '0.8rem' }}
            title="Build serving this page"
          >
            {session.data.version}
          </code>
        )}
        <button
          onClick={() => logout.mutate()}
          disabled={logout.isPending}
          style={{
            marginLeft: session.data.version ? undefined : 'auto',
            padding: '0.35rem 0.75rem',
            cursor: 'pointer',
          }}
        >
          Sign out
        </button>
      </header>
      <Tenants />
    </main>
  )
}
