import { useEffect, useRef, useState } from 'react'

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
        const res = await fetch(`/api/superset/guest-token?dashboard_id=${dashboardId}`)
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
      style={{ width: '100%', height: 'calc(100vh - 220px)', border: 'none', borderRadius: 8 }}
      title="Analytics Dashboard"
    />
  )
}

export default function SupersetEmbed() {
  const [cfg, setCfg] = useState(null)
  const [selectedIdx, setSelectedIdx] = useState(0)

  useEffect(() => {
    fetch('/api/superset/config').then(r => r.json()).then(setCfg)
  }, [])

  if (!cfg) return null

  if (!cfg.dashboards?.length) {
    return (
      <div style={{
        padding: 60, textAlign: 'center', color: '#5f6368',
        background: '#fff', border: '1px solid #dadce0', borderRadius: 8,
      }}>
        <div style={{ fontSize: 32, marginBottom: 12 }}>📊</div>
        <div style={{ fontSize: 15, fontWeight: 500, marginBottom: 8 }}>No dashboard configured</div>
        <div style={{ fontSize: 13 }}>
          Set <code style={{ background: '#f1f3f4', padding: '2px 6px', borderRadius: 4 }}>SUPERSET_DASHBOARDS</code> to embed Superset dashboards.
        </div>
      </div>
    )
  }

  const selected = cfg.dashboards[selectedIdx]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 0 }}>
      {cfg.dashboards.length > 1 && (
        <div style={{ display: 'flex', gap: 0, borderBottom: '1px solid #dadce0', marginBottom: 8 }}>
          {cfg.dashboards.map((d, i) => {
            const active = i === selectedIdx
            return (
              <button
                key={d.id}
                onClick={() => setSelectedIdx(i)}
                style={{
                  background: 'none', border: 'none',
                  borderBottom: active ? '3px solid #34A853' : '3px solid transparent',
                  padding: '8px 20px', fontSize: 13,
                  fontWeight: active ? 600 : 400,
                  color: active ? '#34A853' : '#5f6368',
                  cursor: 'pointer', fontFamily: 'inherit',
                  transition: 'color 0.15s',
                }}
              >
                {d.label}
              </button>
            )
          })}
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
