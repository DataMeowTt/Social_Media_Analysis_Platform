import { getStateColor, formatDate, formatDuration } from '../utils.js'

const CARD = {
  background: 'var(--c-surface)',
  border: '1px solid var(--c-border)',
  borderRadius: 8,
  boxShadow: '0 1px 3px rgba(0,0,0,0.12)',
  padding: '20px 24px',
  display: 'flex',
  alignItems: 'center',
  gap: 24,
}

export default function StatusBanner({ run }) {
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
    <div style={CARD}>
      {/* Status chip */}
      <div
        style={{
          display: 'inline-flex',
          alignItems: 'center',
          gap: 8,
          background: colors.bg,
          border: `1px solid ${colors.border}`,
          borderRadius: 20,
          padding: '6px 16px',
          animation: isRunning ? 'breathe 2s ease-in-out infinite' : 'none',
          flexShrink: 0,
        }}
      >
        <span
          style={{
            width: 10,
            height: 10,
            borderRadius: '50%',
            background: colors.border,
            display: 'inline-block',
          }}
        />
        <span style={{ fontSize: 15, fontWeight: 500, color: colors.text, textTransform: 'capitalize' }}>
          {state.replace('_', ' ')}
        </span>
      </div>

      {/* Meta info */}
      <div style={{ display: 'flex', gap: 32, flexWrap: 'wrap' }}>
        <div>
          <div style={{ fontSize: 11, fontWeight: 500, color: 'var(--c-muted)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 2 }}>
            Started
          </div>
          <div style={{ fontSize: 14, color: 'var(--c-text)' }}>{formatDate(run.start_date)}</div>
        </div>
        <div>
          <div style={{ fontSize: 11, fontWeight: 500, color: 'var(--c-muted)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 2 }}>
            Duration
          </div>
          <div style={{ fontSize: 14, color: 'var(--c-text)' }}>{formatDuration(run.duration_seconds)}</div>
        </div>
        <div>
          <div style={{ fontSize: 11, fontWeight: 500, color: 'var(--c-muted)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 2 }}>
            Triggered by
          </div>
          <div style={{ fontSize: 14, color: 'var(--c-text)' }}>{run.triggered_by ?? '—'}</div>
        </div>
      </div>
    </div>
  )
}
