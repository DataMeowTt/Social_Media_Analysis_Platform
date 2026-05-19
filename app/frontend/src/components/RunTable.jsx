import { getStateColor, formatDate, formatDuration } from '../utils.js'

const CARD = {
  background: 'var(--c-surface)',
  border: '1px solid var(--c-border)',
  borderRadius: 12,
  boxShadow: '0 2px 8px var(--c-shadow)',
  overflow: 'hidden',
}

function StateBadge({ state }) {
  const colors = getStateColor(state ?? 'none')
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 5,
      fontSize: 11, fontWeight: 600,
      color: colors.text,
      background: colors.bg,
      border: `1px solid ${colors.border}28`,
      borderRadius: 10, padding: '2px 9px',
      textTransform: 'capitalize', whiteSpace: 'nowrap',
    }}>
      <span style={{ width: 5, height: 5, borderRadius: '50%', background: colors.border, display: 'inline-block', flexShrink: 0 }} />
      {(state ?? 'none').replace('_', ' ')}
    </span>
  )
}

function truncateId(id) {
  if (!id) return '—'
  return id.length > 20 ? `…${id.slice(-18)}` : id
}

const TH = {
  fontSize: 11, fontWeight: 600, color: 'var(--c-muted)',
  textTransform: 'uppercase', letterSpacing: '0.06em',
  padding: '10px 16px', textAlign: 'left',
  borderBottom: '1px solid var(--c-border)', whiteSpace: 'nowrap',
  background: 'var(--c-app)',
}
const TD = { fontSize: 13, color: 'var(--c-text)', padding: '10px 16px', whiteSpace: 'nowrap' }

export default function RunTable({ runs }) {
  return (
    <div style={CARD}>
      {/* Card header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '20px 24px 16px' }}>
        <div style={{
          width: 30, height: 30, borderRadius: 8,
          background: 'rgba(52,168,83,0.10)',
          display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
        }}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#34A853" strokeWidth="2.5" strokeLinecap="round">
            <line x1="8" y1="6" x2="21" y2="6"/>
            <line x1="8" y1="12" x2="21" y2="12"/>
            <line x1="8" y1="18" x2="21" y2="18"/>
            <line x1="3" y1="6" x2="3.01" y2="6"/>
            <line x1="3" y1="12" x2="3.01" y2="12"/>
            <line x1="3" y1="18" x2="3.01" y2="18"/>
          </svg>
        </div>
        <div>
          <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--c-text)', lineHeight: 1.2 }}>Run History</div>
          <div style={{ fontSize: 11.5, color: 'var(--c-muted)', marginTop: 1 }}>Recent pipeline runs</div>
        </div>
      </div>

      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={TH}>Run ID</th>
              <th style={TH}>State</th>
              <th style={TH}>Started</th>
              <th style={TH}>Duration</th>
              <th style={TH}>Triggered by</th>
            </tr>
          </thead>
          <tbody>
            {runs.length === 0 ? (
              <tr>
                <td colSpan={5} style={{ ...TD, color: 'var(--c-muted)', textAlign: 'center', padding: 36 }}>
                  No runs found
                </td>
              </tr>
            ) : (
              runs.map((run, i) => (
                <tr
                  key={run.run_id ?? i}
                  style={{ background: 'var(--c-surface)', transition: 'background 0.12s', cursor: 'default' }}
                  onMouseEnter={e => e.currentTarget.style.background = 'var(--c-hover)'}
                  onMouseLeave={e => e.currentTarget.style.background = 'var(--c-surface)'}
                >
                  <td style={{ ...TD, fontFamily: 'monospace', fontSize: 11.5, color: 'var(--c-muted)' }}>
                    {truncateId(run.run_id)}
                  </td>
                  <td style={TD}>
                    <StateBadge state={run.state} />
                  </td>
                  <td style={TD}>{formatDate(run.start_date)}</td>
                  <td style={{ ...TD, fontWeight: 500 }}>{formatDuration(run.duration_seconds)}</td>
                  <td style={{ ...TD, color: 'var(--c-muted)' }}>{run.triggered_by ?? '—'}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
