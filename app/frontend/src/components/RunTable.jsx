import { getStateColor, formatDate, formatDuration } from '../utils.js'

const CARD = {
  background: 'var(--c-surface)',
  border: '1px solid var(--c-border)',
  borderRadius: 8,
  boxShadow: '0 1px 3px rgba(0,0,0,0.12)',
  overflow: 'hidden',
}

function StateBadge({ state }) {
  const colors = getStateColor(state ?? 'none')
  return (
    <span
      style={{
        display: 'inline-block',
        fontSize: 11,
        fontWeight: 500,
        color: colors.text,
        background: colors.bg,
        borderRadius: 12,
        padding: '2px 10px',
        textTransform: 'capitalize',
        whiteSpace: 'nowrap',
      }}
    >
      {(state ?? 'none').replace('_', ' ')}
    </span>
  )
}

function truncateId(id) {
  if (!id) return '—'
  return id.length > 20 ? `…${id.slice(-18)}` : id
}

const TH = { fontSize: 11, fontWeight: 500, color: 'var(--c-muted)', textTransform: 'uppercase', letterSpacing: '0.05em', padding: '10px 16px', textAlign: 'left', borderBottom: '1px solid var(--c-border)', whiteSpace: 'nowrap' }
const TD = { fontSize: 13, color: 'var(--c-text)', padding: '10px 16px', whiteSpace: 'nowrap' }

export default function RunTable({ runs }) {
  return (
    <div style={CARD}>
      <div style={{ fontSize: 14, fontWeight: 500, color: 'var(--c-text)', padding: '20px 24px 12px' }}>
        Run History
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
                <td colSpan={5} style={{ ...TD, color: '#5f6368', textAlign: 'center', padding: 32 }}>
                  No runs found
                </td>
              </tr>
            ) : (
              runs.map((run, i) => (
                <tr
                  key={run.run_id ?? i}
                  style={{ background: i % 2 === 0 ? 'var(--c-app)' : 'var(--c-surface)' }}
                >
                  <td style={{ ...TD, fontFamily: 'monospace', fontSize: 12, color: '#5f6368' }}>
                    {truncateId(run.run_id)}
                  </td>
                  <td style={TD}>
                    <StateBadge state={run.state} />
                  </td>
                  <td style={TD}>{formatDate(run.start_date)}</td>
                  <td style={TD}>{formatDuration(run.duration_seconds)}</td>
                  <td style={{ ...TD, color: '#5f6368' }}>{run.triggered_by ?? '—'}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
