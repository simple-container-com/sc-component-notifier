import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { api, describeError, type SessionInfo } from '../api/client'

const card: React.CSSProperties = {
  border: '1px solid #ddd',
  borderRadius: 8,
  padding: '1.5rem',
  maxWidth: 380,
  margin: '4rem auto',
}

export function Login({ session }: { session: SessionInfo }) {
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const qc = useQueryClient()

  const login = useMutation({
    mutationFn: () => api.login(password),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['session'] }),
    onError: (e) => setError(describeError(e)),
  })

  // A deployment with no password configured cannot accept any password.
  // Without saying so, an operator typing the right one sees "wrong
  // password" and goes hunting for a typo that does not exist —
  // `operatorPassword` is optional on the resource.
  if (!session.password_set) {
    return (
      <div style={card}>
        <h1 style={{ fontSize: '1.1rem', margin: 0 }}>Notifier</h1>
        <p style={{ color: '#c00', marginTop: '0.75rem' }}>
          No operator password is configured on this deployment.
        </p>
        <p style={{ color: '#666', fontSize: '0.9em' }}>
          Set <code>operatorPassword</code> on the <code>sc-notifier</code> resource in{' '}
          <code>server.yaml</code> and redeploy.
        </p>
      </div>
    )
  }

  return (
    <form
      style={card}
      onSubmit={(e) => {
        e.preventDefault()
        setError(null)
        login.mutate()
      }}
    >
      <h1 style={{ fontSize: '1.1rem', margin: 0 }}>Notifier</h1>
      <p style={{ color: '#666', margin: '0.25rem 0 1rem', fontSize: '0.9em' }}>
        Alert routing for SC components
      </p>
      <input
        type="password"
        autoFocus
        placeholder="Operator password"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
        style={{ width: '100%', padding: '0.5rem', boxSizing: 'border-box' }}
      />
      {error && <p style={{ color: '#c00', fontSize: '0.85em' }}>{error}</p>}
      <button
        type="submit"
        disabled={!password || login.isPending}
        style={{ width: '100%', padding: '0.5rem', marginTop: '0.75rem', cursor: 'pointer' }}
      >
        {login.isPending ? 'Signing in…' : 'Sign in'}
      </button>
    </form>
  )
}
