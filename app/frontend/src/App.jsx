import { useState, useEffect, useCallback } from 'react'
import Sidebar, { PLATFORMS } from './components/Sidebar.jsx'
import StatusBanner from './components/StatusBanner.jsx'
import TaskFlow from './components/TaskFlow.jsx'
import RunChart from './components/RunChart.jsx'
import RunTable from './components/RunTable.jsx'
import SupersetEmbed from './components/SupersetEmbed.jsx'
import LoginPage from './pages/LoginPage.jsx'
import { apiFetch, formatDate } from './utils.js'

const POLL_INTERVAL = 30_000

const PLATFORM_DAGS = {
  twitter: [
    { id: 'social_media_pipeline',      label: 'Production' },
    { id: 'social_media_pipeline_test', label: 'Test' },
  ],
  youtube: [
    { id: 'youtube_pipeline', label: 'Production' },
  ],
}

const PLATFORM_VIEWS = {
  twitter: ['pipeline', 'analytics'],
  youtube: ['pipeline', 'analytics'],
}

const PLATFORM_LABELS = {
  twitter: 'Twitter',
  youtube: 'YouTube',
}

function SkeletonCard({ height = 120 }) {
  return (
    <div style={{
      background: 'var(--c-surface)', border: '1px solid var(--c-border)',
      borderRadius: 12, height, animation: 'pulse 1.5s ease-in-out infinite',
    }} />
  )
}

function ViewTabs({ views, selected, onChange, platformColor }) {
  const LABELS = { pipeline: 'Pipeline', analytics: 'Analytics' }
  return (
    <div style={{ display: 'flex', gap: 4 }}>
      {views.map(v => {
        const active = v === selected
        return (
          <button
            key={v}
            onClick={() => onChange(v)}
            style={{
              background: active ? platformColor + '18' : 'transparent',
              border: `1px solid ${active ? platformColor + '50' : 'transparent'}`,
              borderRadius: 8,
              padding: '6px 16px',
              fontSize: 13,
              fontWeight: active ? 600 : 400,
              color: active ? platformColor : 'var(--c-muted)',
              cursor: 'pointer',
              transition: 'all 0.15s',
              fontFamily: 'inherit',
            }}
          >
            {LABELS[v]}
          </button>
        )
      })}
    </div>
  )
}

function DagTabs({ dags, selected, onChange }) {
  if (dags.length <= 1) return null
  return (
    <div style={{ display: 'flex', gap: 0, borderBottom: '1px solid var(--c-border)', marginBottom: 4 }}>
      {dags.map(tab => {
        const active = tab.id === selected
        return (
          <button
            key={tab.id}
            onClick={() => onChange(tab.id)}
            style={{
              background: 'none', border: 'none',
              borderBottom: active ? '2px solid var(--c-text)' : '2px solid transparent',
              padding: '9px 18px',
              fontSize: 13,
              fontWeight: active ? 600 : 400,
              color: active ? 'var(--c-text)' : 'var(--c-muted)',
              cursor: 'pointer',
              transition: 'color 0.15s',
              fontFamily: 'inherit',
              marginBottom: -1,
            }}
          >
            {tab.label}
          </button>
        )
      })}
    </div>
  )
}

function RefreshIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="23 4 23 10 17 10" />
      <polyline points="1 20 1 14 7 14" />
      <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
    </svg>
  )
}

export default function App() {
  const [authToken, setAuthToken] = useState(() => localStorage.getItem('authToken'))
  const [selectedPlatform, setSelectedPlatform] = useState('twitter')
  const [selectedView, setSelectedView] = useState('pipeline')
  const [selectedDag, setSelectedDag] = useState('social_media_pipeline')
  const [allStatus, setAllStatus] = useState({})
  const [allRuns, setAllRuns]     = useState({})
  const [lastRefreshed, setLastRefreshed] = useState(null)
  const [loading, setLoading] = useState(true)
  const [dark, setDark] = useState(() => localStorage.getItem('theme') === 'dark')

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', dark ? 'dark' : 'light')
    localStorage.setItem('theme', dark ? 'dark' : 'light')
  }, [dark])

  useEffect(() => {
    const handler = () => setAuthToken(null)
    window.addEventListener('auth:logout', handler)
    return () => window.removeEventListener('auth:logout', handler)
  }, [])

  const handleLogout = useCallback(async () => {
    await apiFetch('/api/auth/logout', { method: 'POST' })
    localStorage.removeItem('authToken')
    setAuthToken(null)
  }, [])

  const fetchData = useCallback(async () => {
    try {
      const [statusRes, runsRes] = await Promise.all([
        apiFetch('/api/status'),
        apiFetch('/api/runs'),
      ])
      if (!statusRes.ok || !runsRes.ok) return
      const [statusData, runsData] = await Promise.all([
        statusRes.json(),
        runsRes.json(),
      ])
      setAllStatus(statusData)
      setAllRuns(runsData)
      setLastRefreshed(new Date())
    } catch (err) {
      console.error('Fetch error:', err)
    } finally {
      setLoading(false)
    }
  }, [])

  const isAnyRunning = Object.values(allStatus).some(s => s?.run?.state === 'running')

  useEffect(() => { if (authToken) fetchData() }, [fetchData, authToken])

  useEffect(() => {
    if (!authToken) return
    const interval = isAnyRunning ? 10_000 : POLL_INTERVAL
    const id = setInterval(fetchData, interval)
    return () => clearInterval(id)
  }, [fetchData, isAnyRunning, authToken])

  const handlePlatformChange = (platform) => {
    setSelectedPlatform(platform)
    const views = PLATFORM_VIEWS[platform] ?? ['analytics']
    if (!views.includes(selectedView)) setSelectedView(views[0])
    const dags = PLATFORM_DAGS[platform]
    if (dags?.length) setSelectedDag(dags[0].id)
  }

  if (!authToken) {
    return <LoginPage onLogin={setAuthToken} />
  }

  const platformColor = { twitter: '#1DA1F2', youtube: '#FF0000' }[selectedPlatform] ?? '#1a73e8'
  const views        = PLATFORM_VIEWS[selectedPlatform] ?? ['analytics']
  const platformDags = PLATFORM_DAGS[selectedPlatform] ?? []
  const status       = allStatus[selectedDag]
  const runs         = allRuns[selectedDag] ?? []

  return (
    <>
      <style>{`
        @keyframes pulse    { 0%,100% { opacity:1 } 50% { opacity:.4 } }
        @keyframes breathe  { 0%,100% { box-shadow: 0 0 0 0 currentColor } 50% { box-shadow: 0 0 0 6px transparent } }
        @keyframes spin     { from { transform: rotate(0deg) } to { transform: rotate(360deg) } }
        .spin { display:inline-block; animation: spin 1s linear infinite; }
        @keyframes flowRight { 0% { transform: translateX(-100%) } 100% { transform: translateX(500%) } }
        .flow-pulse {
          position: absolute; top: 0; left: 0;
          width: 40%; height: 100%;
          background: linear-gradient(90deg, transparent, #1a73e8, #60a5fa, transparent);
          animation: flowRight 1s linear infinite;
          border-radius: 2px;
        }
        .topbar-refresh:hover { background: var(--c-hover) !important; border-color: var(--c-border) !important; }
      `}</style>

      <div style={{ display: 'flex', minHeight: '100vh' }}>

        {/* Dark Sidebar */}
        <Sidebar
          selectedPlatform={selectedPlatform}
          onChange={handlePlatformChange}
          dark={dark}
          onToggleDark={() => setDark(d => !d)}
          onLogout={handleLogout}
          lastRefreshed={lastRefreshed}
          onRefresh={fetchData}
        />

        {/* Right column */}
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minWidth: 0, background: 'var(--c-app)' }}>

          {/* Topbar */}
          <div style={{
            background: 'var(--c-surface)',
            borderBottom: '1px solid var(--c-border)',
            padding: '0 28px',
            height: 52,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            flexShrink: 0,
            position: 'sticky',
            top: 0,
            zIndex: 10,
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 7 }}>
                <div style={{ width: 8, height: 8, borderRadius: '50%', background: platformColor, flexShrink: 0 }} />
                <span style={{ fontSize: 14, fontWeight: 600, color: 'var(--c-text)' }}>
                  {PLATFORM_LABELS[selectedPlatform] ?? selectedPlatform}
                </span>
                <span style={{ color: 'var(--c-border)', fontSize: 18, fontWeight: 200, lineHeight: 1 }}>/</span>
              </div>
              <ViewTabs views={views} selected={selectedView} onChange={setSelectedView} platformColor={platformColor} />
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              {lastRefreshed && (
                <span style={{ fontSize: 12, color: 'var(--c-muted)' }}>
                  Updated {formatDate(lastRefreshed.toISOString())}
                </span>
              )}
              <button
                onClick={fetchData}
                title="Refresh"
                className="topbar-refresh"
                style={{
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  width: 30, height: 30, borderRadius: 7,
                  background: 'transparent',
                  border: '1px solid transparent',
                  color: 'var(--c-muted)', cursor: 'pointer',
                  transition: 'all 0.15s',
                }}
              >
                <RefreshIcon />
              </button>
            </div>
          </div>

          {/* Main content */}
          <main style={{ flex: 1, padding: '28px', display: 'flex', flexDirection: 'column', gap: 20, minWidth: 0 }}>
            {selectedView === 'analytics' ? (
              <SupersetEmbed platform={selectedPlatform} />
            ) : (
              <>
                <DagTabs dags={platformDags} selected={selectedDag} onChange={setSelectedDag} />

                {loading ? (
                  <>
                    <SkeletonCard height={100} />
                    <SkeletonCard height={160} />
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>
                      <SkeletonCard height={280} />
                      <SkeletonCard height={280} />
                    </div>
                  </>
                ) : (
                  <>
                    <StatusBanner run={status?.run} />
                    <TaskFlow tasks={status?.tasks ?? []} />
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))', gap: 20 }}>
                      <RunChart runs={runs} />
                      <RunTable runs={runs} />
                    </div>
                  </>
                )}
              </>
            )}
          </main>
        </div>
      </div>
    </>
  )
}
