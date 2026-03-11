import { FormEvent, useEffect, useReducer, useState } from 'react'
import Dashboard, { extractLabs, type LabOption } from './Dashboard'
import './App.css'

const STORAGE_KEY = 'api_key'

interface Item {
  id: number
  type: string
  title: string
  created_at: string
}

type Page = 'items' | 'dashboard'

type FetchState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; items: Item[] }
  | { status: 'error'; message: string }

type FetchAction =
  | { type: 'fetch_start' }
  | { type: 'fetch_success'; data: Item[] }
  | { type: 'fetch_error'; message: string }

function fetchReducer(_state: FetchState, action: FetchAction): FetchState {
  switch (action.type) {
    case 'fetch_start':
      return { status: 'loading' }
    case 'fetch_success':
      return { status: 'success', items: action.data }
    case 'fetch_error':
      return { status: 'error', message: action.message }
  }
}

function App() {
  const [token, setToken] = useState(() => localStorage.getItem(STORAGE_KEY) ?? '')
  const [draft, setDraft] = useState('')
  const [page, setPage] = useState<Page>('items')
  const [fetchState, dispatch] = useReducer(fetchReducer, { status: 'idle' })

  useEffect(() => {
    if (!token) return

    dispatch({ type: 'fetch_start' })

    fetch('/items/', {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: Item[]) => dispatch({ type: 'fetch_success', data }))
      .catch((err: Error) =>
        dispatch({ type: 'fetch_error', message: err.message }),
      )
  }, [token])

  function handleConnect(e: FormEvent) {
    e.preventDefault()
    const trimmed = draft.trim()
    if (!trimmed) return
    localStorage.setItem(STORAGE_KEY, trimmed)
    setToken(trimmed)
  }

  function handleDisconnect() {
    localStorage.removeItem(STORAGE_KEY)
    setToken('')
    setDraft('')
    setPage('items')
  }

  if (!token) {
    return (
      <form className="token-form" onSubmit={handleConnect}>
        <h1>API Key</h1>
        <p>Enter your API key to connect.</p>
        <input
          type="password"
          placeholder="Token"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
        />
        <button type="submit">Connect</button>
      </form>
    )
  }

  const labs: LabOption[] =
    fetchState.status === 'success' ? extractLabs(fetchState.items) : []

  return (
    <div className="app-shell">
      <header className="app-header">
        <div>
          <p className="eyebrow">Learning Management Service</p>
          <h1>{page === 'items' ? 'Items' : 'Analytics Dashboard'}</h1>
        </div>
        <div className="header-actions">
          <nav className="page-nav" aria-label="Page navigation">
            <button
              className={page === 'items' ? 'nav-button nav-button-active' : 'nav-button'}
              onClick={() => setPage('items')}
              type="button"
            >
              Items
            </button>
            <button
              className={
                page === 'dashboard' ? 'nav-button nav-button-active' : 'nav-button'
              }
              onClick={() => setPage('dashboard')}
              type="button"
            >
              Dashboard
            </button>
          </nav>
          <button className="btn-disconnect" onClick={handleDisconnect}>
            Disconnect
          </button>
        </div>
      </header>

      {page === 'items' && (
        <>
          {fetchState.status === 'loading' && <p className="status-card">Loading items...</p>}
          {fetchState.status === 'error' && (
            <p className="status-card status-card-error">Error: {fetchState.message}</p>
          )}

          {fetchState.status === 'success' && (
            <section className="panel table-panel">
              <div className="panel-header">
                <h2>Items</h2>
                <span>{fetchState.items.length} records</span>
              </div>
              <table>
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Item Type</th>
                    <th>Title</th>
                    <th>Created at</th>
                  </tr>
                </thead>
                <tbody>
                  {fetchState.items.map((item) => (
                    <tr key={item.id}>
                      <td>{item.id}</td>
                      <td>{item.type}</td>
                      <td>{item.title}</td>
                      <td>{item.created_at}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </section>
          )}
        </>
      )}

      {page === 'dashboard' && <Dashboard token={token} labs={labs} />}
    </div>
  )
}

export default App
