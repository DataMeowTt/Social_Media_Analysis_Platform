import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
} from 'recharts'
import { getBarFill } from '../utils.js'

const CARD = {
  background: 'var(--c-surface)',
  border: '1px solid var(--c-border)',
  borderRadius: 8,
  boxShadow: '0 1px 3px rgba(0,0,0,0.12)',
  padding: '20px 24px',
}

function CustomTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const d = payload[0].payload
  return (
    <div style={{ background: 'var(--c-surface)', border: '1px solid var(--c-border)', borderRadius: 6, padding: '8px 12px', fontSize: 12 }}>
      <div style={{ fontWeight: 500, color: 'var(--c-text)', marginBottom: 4 }}>Run #{d.label}</div>
      <div style={{ color: 'var(--c-muted)' }}>Duration: <strong>{d.durationLabel}</strong></div>
      <div style={{ color: 'var(--c-muted)', textTransform: 'capitalize' }}>State: <strong>{d.state}</strong></div>
    </div>
  )
}

export default function RunChart({ runs }) {
  const last10 = [...(runs ?? [])].slice(-10)

  const data = last10.map((r, i) => {
    const secs = r.duration_seconds ?? 0
    const mins = +(secs / 60).toFixed(2)
    const m = Math.floor(secs / 60)
    const s = Math.round(secs % 60)
    return {
      label: i + 1,
      mins,
      state: r.state ?? 'none',
      durationLabel: m > 0 ? `${m}m ${s}s` : `${s}s`,
    }
  })

  return (
    <div style={CARD}>
      <div style={{ fontSize: 14, fontWeight: 500, color: 'var(--c-text)', marginBottom: 16 }}>
        Run Duration (last 10)
      </div>

      {data.length === 0 ? (
        <div style={{ height: 200, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#5f6368', fontSize: 13 }}>
          No run data
        </div>
      ) : (
        <ResponsiveContainer width="100%" height={220}>
          <BarChart data={data} margin={{ top: 4, right: 8, left: -8, bottom: 0 }} barCategoryGap="30%">
            <XAxis
              dataKey="label"
              tick={{ fontSize: 11, fill: 'var(--c-muted)' }}
              axisLine={{ stroke: '#dadce0' }}
              tickLine={false}
              label={{ value: 'Run #', position: 'insideBottom', offset: -2, fontSize: 11, fill: '#9aa0a6' }}
            />
            <YAxis
              tick={{ fontSize: 11, fill: 'var(--c-muted)' }}
              axisLine={false}
              tickLine={false}
              unit=" m"
            />
            <Tooltip content={<CustomTooltip />} cursor={{ fill: 'var(--c-hover)' }} />
            <Bar dataKey="mins" radius={[4, 4, 0, 0]}>
              {data.map((entry, i) => (
                <Cell key={i} fill={getBarFill(entry.state)} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}
