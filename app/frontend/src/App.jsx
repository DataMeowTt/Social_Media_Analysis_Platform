import { useState, useEffect, useCallback } from 'react'
import Header from './components/Header.jsx'
import StatusBanner from './components/StatusBanner.jsx'
import TaskFlow from './components/TaskFlow.jsx'
import RunChart from './components/RunChart.jsx'
import RunTable from './components/RunTable.jsx'
import SupersetEmbed from './components/SupersetEmbed.jsx'

const POLL_INTERVAL = 30_000

const VIEW_TABS = [
  { id: 'pipeline',   label: 'Pipeline' },
  { id: 'analytics',  label: 'Analytics' },
]

const DAG_TABS = [
  { id: 'social_media_pipeline',      label: 'Production' },
  { id: 'social_media_pipeline_test', label: 'Test' },
]

function SkeletonCard({ height = 120 }) {
  return (
    <div style={{
      background: 'var(--c-surface)', border: '1px solid var(--c-border)',
      borderRadius: 8, height, animation: 'pulse 1.5s ease-in-out infinite',
    }} />
  )
}

function ViewTabs({ selected, onChange }) {
  return (
    <div style={{ display: 'flex', gap: 0, borderBottom: '2px solid var(--c-border)', marginBottom: 8 }}>
      {VIEW_TABS.map(tab => {
        const active = tab.id === selected
        return (
          <button
            key={tab.id}
            onClick={() => onChange(tab.id)}
            style={{
              background: 'none',
              border: 'none',
              borderBottom: active ? '3px solid #1a73e8' : '3px solid transparent',
              padding: '10px 24px',
              fontSize: 15,
              fontWeight: active ? 600 : 400,
              color: active ? '#1a73e8' : 'var(--c-muted)',
              cursor: 'pointer',
              transition: 'color 0.15s',
              fontFamily: 'inherit',
            }}
          >
            {tab.label}
          </button>
        )
      })}
    </div>
  )
}

function DagTabs({ selected, onChange }) {
  return (
    <div style={{ display: 'flex', gap: 0, borderBottom: '1px solid var(--c-border)', marginBottom: 4 }}>
      {DAG_TABS.map(tab => {
        const active = tab.id === selected
        return (
          <button
            key={tab.id}
            onClick={() => onChange(tab.id)}
            style={{
              background: 'none',
              border: 'none',
              borderBottom: active ? '3px solid #1a73e8' : '3px solid transparent',
              padding: '10px 20px',
              fontSize: 14,
              fontWeight: active ? 600 : 400,
              color: active ? '#1a73e8' : 'var(--c-muted)',
              cursor: 'pointer',
              transition: 'color 0.15s',
              fontFamily: 'inherit',
            }}
          >
            {tab.label}
          </button>
        )
      })}
    </div>
  )
}

export default function App() {
  const [allStatus, setAllStatus] = useState({})
  const [allRuns, setAllRuns]     = useState({})
  const [selectedView, setSelectedView] = useState('pipeline')
  const [selectedDag, setSelectedDag] = useState('social_media_pipeline')
  const [lastRefreshed, setLastRefreshed] = useState(null)
  const [loading, setLoading] = useState(true)
  const [dark, setDark] = useState(() => localStorage.getItem('theme') === 'dark')

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', dark ? 'dark' : 'light')
    localStorage.setItem('theme', dark ? 'dark' : 'light')
  }, [dark])

  const fetchData = useCallback(async () => {
    try {
      const [statusRes, runsRes] = await Promise.all([
        fetch('/api/status'),
        fetch('/api/runs'),
      ])
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

  useEffect(() => { fetchData() }, [fetchData])

  useEffect(() => {
    const interval = isAnyRunning ? 10_000 : POLL_INTERVAL
    const id = setInterval(fetchData, interval)
    return () => clearInterval(id)
  }, [fetchData, isAnyRunning])

  const status = allStatus[selectedDag]
  const runs   = allRuns[selectedDag] ?? []

  return (
    <>
      <style>{`
        @keyframes pulse   { 0%,100% { opacity:1 } 50% { opacity:.4 } }
        @keyframes breathe { 0%,100% { opacity:1 } 50% { opacity:.6 } }
        @keyframes spin      { from { transform: rotate(0deg) } to { transform: rotate(360deg) } }
        .spin { display:inline-block; animation: spin 1s linear infinite; }
        @keyframes flowRight { 0% { transform: translateX(-100%) } 100% { transform: translateX(500%) } }
        .flow-pulse {
          position: absolute; top: 0; left: 0;
          width: 40%; height: 100%;
          background: linear-gradient(90deg, transparent, #1a73e8, #60a5fa, transparent);
          animation: flowRight 1s linear infinite;
          border-radius: 2px;
        }
      `}</style>

      <Header lastRefreshed={lastRefreshed} onRefresh={fetchData} dark={dark} onToggleDark={() => setDark(d => !d)} />

      <main style={{ maxWidth: 1200, margin: '0 auto', padding: '24px 16px', display: 'flex', flexDirection: 'column', gap: 20 }}>
        <ViewTabs selected={selectedView} onChange={setSelectedView} />

        {selectedView === 'analytics' ? (
          <SupersetEmbed />
        ) : (
          <>
            <DagTabs selected={selectedDag} onChange={setSelectedDag} />

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
    </>
  )
}
