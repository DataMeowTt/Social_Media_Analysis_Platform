import { getStateColor, formatDate, formatDuration } from '../utils.js'

const STATE_ICONS = {
  success: (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="20 6 9 17 4 12"/>
    </svg>
  ),
  running: (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
      <circle cx="12" cy="12" r="10"/>
      <polyline points="12 6 12 12 16 14"/>
    </svg>
  ),
  failed: (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
      <line x1="18" y1="6" x2="6" y2="18"/>
      <line x1="6" y1="6" x2="18" y2="18"/>
    </svg>
  ),
  upstream_failed: (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
      <line x1="18" y1="6" x2="6" y2="18"/>
      <line x1="6" y1="6" x2="18" y2="18"/>
    </svg>
  ),
  queued: (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
      <circle cx="12" cy="12" r="10"/>
      <polyline points="12 6 12 12 16 14"/>
    </svg>
  ),
  scheduled: (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
      <rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/>
    </svg>
  ),
}

function Metric({ label, value, large }) {
  return (
    <div>
      <div style={{
        fontSize: 10.5, fontWeight: 600, color: 'var(--c-muted)',
        textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 5,
      }}>
        {label}
      </div>
      <div style={{
        fontSize: large ? 26 : 14,
        fontWeight: large ? 700 : 400,
        color: 'var(--c-text)',
        letterSpacing: large ? '-0.02em' : 0,
        lineHeight: 1.1,
      }}>
        {value}
      </div>
    </div>
  )
}

export default function StatusBanner({ run }) {
  const CARD = {
    background: 'var(--c-surface)',
    border: '1px solid var(--c-border)',
    borderRadius: 12,
    boxShadow: '0 2px 8px var(--c-shadow)',
    padding: '20px 24px',
    display: 'flex',
    alignItems: 'center',
    gap: 24,
  }

  if (!run) {
    return (
      <div style={CARD}>
        <span style={{ color: 'var(--c-muted)', fontSize: 14 }}>No run data available.</span>
      </div>
    )
  }

  const state = run.state ?? 'none'
  const colors = getStateColor(state)
  const isRunning = state === 'running'

  return (
    <div style={{ ...CARD, borderLeft: `4px solid ${colors.border}` }}>

      {/* State circle */}
      <div style={{ flexShrink: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 7, minWidth: 76 }}>
        <div
          className={isRunning ? 'spin' : undefined}
          style={{
            width: 52, height: 52, borderRadius: '50%',
            background: colors.bg,
            border: `2px solid ${colors.border}`,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            color: colors.text,
          }}
        >
          {STATE_ICONS[state] ?? STATE_ICONS.queued}
        </div>
        <span style={{
          fontSize: 11.5, fontWeight: 700,
          color: colors.text,
          textTransform: 'capitalize',
          textAlign: 'center',
          lineHeight: 1.2,
          letterSpacing: '0.02em',
        }}>
          {state.replace(/_/g, ' ')}
        </span>
      </div>

      {/* Divider */}
      <div style={{ width: 1, height: 64, background: 'var(--c-border)', flexShrink: 0 }} />

      {/* Metrics */}
      <div style={{ display: 'flex', gap: 40, flexWrap: 'wrap', flex: 1, alignItems: 'center' }}>
        <Metric label="Duration" value={formatDuration(run.duration_seconds)} large />
        <Metric label="Started" value={formatDate(run.start_date)} />
        <Metric label="Triggered by" value={run.triggered_by ?? '—'} />
      </div>
    </div>
  )
}
