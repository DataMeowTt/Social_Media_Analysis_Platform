import { useEffect, useRef, useState } from 'react'
import { apiFetch } from '../utils.js'

function DashboardIframe({ dashboardId, supersetUrl }) {
  const iframeRef = useRef(null)

  useEffect(() => {
    if (!iframeRef.current) return
    const iframe = iframeRef.current
    let port2 = null

    const handleLoad = async () => {
      const channel = new MessageChannel()
      port2 = channel.port2

      port2.addEventListener('message', (event) => {
        const { switchboardAction, method, messageId } = event.data
        if (switchboardAction !== 'get') return
        const result = method === 'getScrollSize'
          ? { width: document.body.scrollWidth, height: document.body.scrollHeight }
          : undefined
        port2.postMessage({ switchboardAction: 'reply', messageId, result })
      })
      port2.start()

      iframe.contentWindow.postMessage(
        { type: '__embedded_comms__', handshake: 'port transfer' },
        supersetUrl,
        [channel.port1],
      )

      try {
        const res = await apiFetch(`/api/superset/guest-token?dashboard_id=${dashboardId}`)
        const { token } = await res.json()
        port2.postMessage({
          switchboardAction: 'emit',
          method: 'guestToken',
          args: { guestToken: token },
        })
      } catch (err) {
        console.error('Superset guest token error:', err)
      }
    }

    iframe.addEventListener('load', handleLoad)
    return () => {
      iframe.removeEventListener('load', handleLoad)
      port2?.close()
    }
  }, [dashboardId, supersetUrl])

  return (
    <iframe
      ref={iframeRef}
      src={`${supersetUrl}/embedded/${dashboardId}`}
      style={{ width: '100%', height: 'calc(100vh - 160px)', border: 'none', borderRadius: 8 }}
      title="Analytics Dashboard"
    />
  )
}

const TAB_BTN = (active, color) => ({
  background: 'none', border: 'none',
  borderBottom: active ? `3px solid ${color}` : '3px solid transparent',
  padding: '8px 20px', fontSize: 13,
  fontWeight: active ? 600 : 400,
  color: active ? color : 'var(--c-muted)',
  cursor: 'pointer', fontFamily: 'inherit',
  transition: 'color 0.15s',
})

const PLATFORM_COLORS = { twitter: '#1DA1F2', youtube: '#FF0000' }

export default function SupersetEmbed({ platform }) {
  const [cfg, setCfg] = useState(null)
  const [selectedIdx, setSelectedIdx] = useState(0)

  useEffect(() => {
    apiFetch('/api/superset/config').then(r => r.json()).then(setCfg)
    setSelectedIdx(0)
  }, [platform])

  if (!cfg) return null

  const dashboards = cfg.platforms?.[platform] ?? []
  const color = PLATFORM_COLORS[platform] ?? '#1a73e8'

  if (!dashboards.length) {
    return (
      <div style={{
        padding: 60, textAlign: 'center', color: 'var(--c-muted)',
        background: 'var(--c-surface)', border: '1px solid var(--c-border)', borderRadius: 8,
      }}>
        <div style={{ fontSize: 32, marginBottom: 12 }}>📊</div>
        <div style={{ fontSize: 15, fontWeight: 500, marginBottom: 8, color: 'var(--c-text)' }}>
          No dashboard configured
        </div>
        <div style={{ fontSize: 13 }}>
          Set <code style={{ background: 'var(--c-hover)', padding: '2px 6px', borderRadius: 4 }}>
            SUPERSET_DASHBOARDS_{platform.toUpperCase()}
          </code> in .env to embed dashboards.
        </div>
      </div>
    )
  }

  const selected = dashboards[selectedIdx]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 0 }}>
      {dashboards.length > 1 && (
        <div style={{ display: 'flex', gap: 0, borderBottom: '1px solid var(--c-border)', marginBottom: 8 }}>
          {dashboards.map((d, i) => (
            <button
              key={d.id}
              onClick={() => setSelectedIdx(i)}
              style={TAB_BTN(i === selectedIdx, color)}
            >
              {d.label}
            </button>
          ))}
        </div>
      )}
      <DashboardIframe
        key={selected.id}
        dashboardId={selected.id}
        supersetUrl={cfg.superset_url}
      />
    </div>
  )
}
