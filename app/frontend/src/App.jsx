import { useState, useEffect, useCallback } from 'react'
import Sidebar, { PLATFORMS } from './components/Sidebar.jsx'
import StatusBanner from './components/StatusBanner.jsx'
import TaskFlow from './components/TaskFlow.jsx'
import RunChart from './components/RunChart.jsx'
import RunTable from './components/RunTable.jsx'
import SupersetEmbed from './components/SupersetEmbed.jsx'
import ThreadInsights from './components/ThreadInsights.jsx'
import FacebookCharts from './components/FacebookCharts.jsx'
import LoginPage from './pages/LoginPage.jsx'
import LogDrawer from './components/LogDrawer.jsx'
import { apiFetch, formatDate } from './utils.js'

const POLL_INTERVAL = 30_000

const PLATFORM_DAGS = {
  twitter: [
    { id: 'twitter_pipeline',      label: 'Production' },
    { id: 'twitter_pipeline_test', label: 'Test' },
  ],
  youtube: [
    { id: 'youtube_pipeline', label: 'Production' },
  ],
  facebook: [
    { id: 'facebook_pipeline', label: 'Production' },
  ],
}

const PLATFORM_VIEWS = {
  twitter: ['pipeline', 'analytics'],
  youtube: ['pipeline', 'analytics', 'thread_insights'],
  facebook: ['pipeline', 'analytics'],
}

const PLATFORM_LABELS = {
  twitter: 'Twitter',
  youtube: 'YouTube',
  facebook: 'Facebook',
}

const TRIGGER_CONF_DEFAULTS = {
  twitter:  { api_key: '', query: '', query_type: 'LATEST', tweets_number: 100 },
  youtube:  { video_id: '' },
  facebook: { mode: 'post', post_urls: '', page_name: 'VinFast VF3 Việt Nam - VINNO CLUB', group_url: '', limit: 10, min_comments: 5 },
}

const _INPUT = {
  width: '100%', padding: '7px 10px', borderRadius: 7,
  border: '1px solid var(--c-border)', background: 'var(--c-app)',
  color: 'var(--c-text)', fontSize: 13, fontFamily: 'inherit',
  boxSizing: 'border-box', outline: 'none',
}
const _LABEL = {
  fontSize: 11, fontWeight: 600, color: 'var(--c-muted)',
  letterSpacing: '0.04em', textTransform: 'uppercase',
  marginBottom: 4, display: 'block',
}
const _FIELD = { display: 'flex', flexDirection: 'column' }
const _REQ   = { color: '#f28b82', marginLeft: 2 }
const _OPT   = { color: 'var(--c-muted)', fontWeight: 400, textTransform: 'none', fontSize: 11 }

function TriggerForm({ platform, conf, onChange }) {
  const set = (key, val) => onChange({ ...conf, [key]: val })

  if (platform === 'twitter') return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
      <div style={_FIELD}>
        <label style={_LABEL}>API Key<span style={_REQ}>*</span></label>
        <input
          type="password" style={_INPUT}
          value={conf.api_key ?? ''}
          placeholder="Enter Twitter API key"
          onChange={e => set('api_key', e.target.value)}
          autoComplete="off"
        />
      </div>
      <div style={_FIELD}>
        <label style={_LABEL}>Query <span style={_OPT}>(optional)</span></label>
        <textarea
          style={{ ..._INPUT, resize: 'vertical', minHeight: 60, lineHeight: 1.5 }}
          value={conf.query ?? ''}
          placeholder="Default: VinFast OR Tesla OR BYD… lang:en"
          onChange={e => set('query', e.target.value)}
        />
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
        <div style={_FIELD}>
          <label style={_LABEL}>Query Type</label>
          <select style={_INPUT} value={conf.query_type ?? 'LATEST'} onChange={e => set('query_type', e.target.value)}>
            <option value="LATEST">Latest</option>
            <option value="TOP">Top</option>
          </select>
        </div>
        <div style={_FIELD}>
          <label style={_LABEL}>Tweet Count</label>
          <input
            type="number" min={1} max={10000} style={_INPUT}
            value={conf.tweets_number ?? 100}
            onChange={e => set('tweets_number', Number(e.target.value))}
          />
        </div>
      </div>
    </div>
  )

  if (platform === 'youtube') return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
      <div style={_FIELD}>
        <label style={_LABEL}>Video ID<span style={_REQ}>*</span></label>
        <input
          type="text" style={_INPUT}
          value={conf.video_id ?? ''}
          placeholder="e.g. dQw4w9WgXcQ"
          onChange={e => set('video_id', e.target.value)}
        />
      </div>
    </div>
  )

  if (platform === 'facebook') return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
      <div style={_FIELD}>
        <label style={_LABEL}>Mode</label>
        <select style={_INPUT} value={conf.mode ?? 'post'} onChange={e => set('mode', e.target.value)}>
          <option value="post">Post URLs</option>
          <option value="group">Group</option>
        </select>
      </div>
      {conf.mode === 'group' ? (
        <>
          <div style={_FIELD}>
            <label style={_LABEL}>Group URL<span style={_REQ}>*</span></label>
            <input
              type="text" style={_INPUT}
              value={conf.group_url ?? ''}
              placeholder="https://www.facebook.com/groups/..."
              onChange={e => set('group_url', e.target.value)}
            />
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
            <div style={_FIELD}>
              <label style={_LABEL}>Limit</label>
              <input type="number" min={1} style={_INPUT} value={conf.limit ?? 10} onChange={e => set('limit', Number(e.target.value))} />
            </div>
            <div style={_FIELD}>
              <label style={_LABEL}>Min Comments</label>
              <input type="number" min={0} style={_INPUT} value={conf.min_comments ?? 5} onChange={e => set('min_comments', Number(e.target.value))} />
            </div>
          </div>
        </>
      ) : (
        <>
          <div style={_FIELD}>
            <label style={_LABEL}>Post URLs<span style={_REQ}>*</span></label>
            <textarea
              style={{ ..._INPUT, resize: 'vertical', minHeight: 60, lineHeight: 1.5 }}
              value={conf.post_urls ?? ''}
              placeholder="https://www.facebook.com/..."
              onChange={e => set('post_urls', e.target.value)}
            />
          </div>
          <div style={_FIELD}>
            <label style={_LABEL}>Page Name <span style={_OPT}>(optional)</span></label>
            <input
              type="text" style={_INPUT}
              value={conf.page_name ?? ''}
              placeholder="VinFast VF3 Việt Nam - VINNO CLUB"
              onChange={e => set('page_name', e.target.value)}
            />
          </div>
        </>
      )}
    </div>
  )

  return null
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
  const LABELS = { pipeline: 'Pipeline', analytics: 'Analytics', thread_insights: 'Thread Insights' }
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
  const [selectedPlatform, setSelectedPlatform] = useState(() => localStorage.getItem('nav_platform') ?? 'twitter')
  const [selectedView, setSelectedView] = useState(() => localStorage.getItem('nav_view') ?? 'pipeline')
  const [selectedDag, setSelectedDag] = useState(() => localStorage.getItem('nav_dag') ?? 'twitter_pipeline')
  const [allStatus, setAllStatus] = useState({})
  const [allRuns, setAllRuns]     = useState({})
  const [lastRefreshed, setLastRefreshed] = useState(null)
  const [loading, setLoading] = useState(true)
  const [dark, setDark] = useState(() => localStorage.getItem('theme') === 'dark')
  const [triggering, setTriggering] = useState(false)
  const [triggerMsg, setTriggerMsg] = useState(null)
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [logTask, setLogTask] = useState(null)
  const [triggerConf, setTriggerConf] = useState({})

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', dark ? 'dark' : 'light')
    localStorage.setItem('theme', dark ? 'dark' : 'light')
  }, [dark])

  useEffect(() => { localStorage.setItem('nav_platform', selectedPlatform) }, [selectedPlatform])
  useEffect(() => { localStorage.setItem('nav_view', selectedView) }, [selectedView])
  useEffect(() => { localStorage.setItem('nav_dag', selectedDag) }, [selectedDag])

  useEffect(() => {
    if (confirmOpen) setTriggerConf(TRIGGER_CONF_DEFAULTS[selectedPlatform] ?? {})
  }, [confirmOpen, selectedPlatform])

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

  const canTrigger = !triggering && (() => {
    if (selectedPlatform === 'twitter')  return !!triggerConf.api_key?.trim()
    if (selectedPlatform === 'youtube')  return !!triggerConf.video_id?.trim()
    if (selectedPlatform === 'facebook') return triggerConf.mode === 'group'
      ? !!triggerConf.group_url?.trim()
      : !!triggerConf.post_urls?.trim()
    return true
  })()

  useEffect(() => { if (authToken) fetchData() }, [fetchData, authToken])

  useEffect(() => {
    if (!authToken) return
    const interval = isAnyRunning ? 10_000 : POLL_INTERVAL
    const id = setInterval(fetchData, interval)
    return () => clearInterval(id)
  }, [fetchData, isAnyRunning, authToken])

  const handleTrigger = useCallback(async () => {
    setConfirmOpen(false)
    setTriggering(true)
    setTriggerMsg(null)
    try {
      const res = await apiFetch(`/api/trigger/${selectedDag}`, { method: 'POST' })
      if (res.ok) {
        setTriggerMsg({ ok: true, text: 'Pipeline triggered' })
        setTimeout(fetchData, 1500)
      } else {
        const data = await res.json().catch(() => ({}))
        setTriggerMsg({ ok: false, text: data.detail ?? 'Trigger failed' })
      }
    } catch {
      setTriggerMsg({ ok: false, text: 'Network error' })
    } finally {
      setTriggering(false)
      setTimeout(() => setTriggerMsg(null), 4000)
    }
  }, [selectedDag, fetchData])

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
              {triggerMsg && (
                <span style={{ fontSize: 12, color: triggerMsg.ok ? '#137333' : '#c5221f', fontWeight: 500 }}>
                  {triggerMsg.text}
                </span>
              )}
              {lastRefreshed && !triggerMsg && (
                <span style={{ fontSize: 12, color: 'var(--c-muted)' }}>
                  Updated {formatDate(lastRefreshed.toISOString())}
                </span>
              )}
              {selectedView === 'pipeline' && (
                <button
                  onClick={() => setConfirmOpen(true)}
                  disabled={triggering || status?.run?.state === 'running'}
                  style={{
                    display: 'flex', alignItems: 'center', gap: 6,
                    padding: '0 14px', height: 30, borderRadius: 7,
                    background: platformColor,
                    border: 'none',
                    color: '#fff',
                    fontSize: 12, fontWeight: 600,
                    cursor: (triggering || status?.run?.state === 'running') ? 'not-allowed' : 'pointer',
                    opacity: (triggering || status?.run?.state === 'running') ? 0.55 : 1,
                    transition: 'opacity 0.15s',
                    fontFamily: 'inherit',
                  }}
                >
                  {triggering ? <span className="spin" style={{ display: 'inline-block' }}>↻</span> : '▶'}
                  {triggering ? 'Triggering…' : 'Run Pipeline'}
                </button>
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
            {selectedView === 'thread_insights' ? (
              <ThreadInsights />
            ) : selectedView === 'analytics' && selectedPlatform === 'facebook' ? (
              <>
                <SupersetEmbed platform="facebook" />
                <FacebookCharts />
              </>
            ) : selectedView === 'analytics' ? (
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
                    <TaskFlow tasks={status?.tasks ?? []} onTaskClick={setLogTask} />
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

      {logTask && status?.run?.run_id && (
        <LogDrawer
          dagId={selectedDag}
          runId={status.run.run_id}
          task={logTask}
          onClose={() => setLogTask(null)}
        />
      )}

      {confirmOpen && (
        <div
          onClick={() => setConfirmOpen(false)}
          style={{
            position: 'fixed', inset: 0, zIndex: 100,
            background: 'rgba(0,0,0,0.45)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}
        >
          <div
            onClick={e => e.stopPropagation()}
            style={{
              background: 'var(--c-surface)',
              border: '1px solid var(--c-border)',
              borderRadius: 14,
              padding: '28px 32px',
              width: 460,
              maxHeight: '85vh',
              overflowY: 'auto',
              boxShadow: '0 8px 32px rgba(0,0,0,0.22)',
            }}
          >
            <div style={{ fontSize: 15, fontWeight: 700, color: 'var(--c-text)', marginBottom: 4 }}>
              Run {PLATFORM_LABELS[selectedPlatform]} pipeline
            </div>
            <div style={{ fontSize: 12, color: 'var(--c-muted)', marginBottom: 20 }}>
              DAG: <span style={{ color: 'var(--c-text)', fontFamily: 'monospace' }}>{selectedDag}</span>
            </div>

            <TriggerForm platform={selectedPlatform} conf={triggerConf} onChange={setTriggerConf} />

            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10, marginTop: 24 }}>
              <button
                onClick={() => setConfirmOpen(false)}
                style={{
                  padding: '8px 18px', borderRadius: 8,
                  border: '1px solid var(--c-border)',
                  background: 'transparent',
                  color: 'var(--c-text)',
                  fontSize: 13, fontWeight: 500,
                  cursor: 'pointer', fontFamily: 'inherit',
                }}
              >
                Cancel
              </button>
              <button
                onClick={handleTrigger}
                disabled={!canTrigger}
                style={{
                  padding: '8px 18px', borderRadius: 8,
                  border: 'none',
                  background: canTrigger ? platformColor : 'var(--c-border)',
                  color: canTrigger ? '#fff' : 'var(--c-muted)',
                  fontSize: 13, fontWeight: 600,
                  cursor: canTrigger ? 'pointer' : 'not-allowed',
                  fontFamily: 'inherit',
                  transition: 'background 0.15s, color 0.15s',
                }}
              >
                Run now
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  )
}
