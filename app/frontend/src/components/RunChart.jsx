import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid,
} from 'recharts'
import { getBarFill } from '../utils.js'

const CARD = {
  background: 'var(--c-surface)',
  border: '1px solid var(--c-border)',
  borderRadius: 12,
  boxShadow: '0 2px 8px var(--c-shadow)',
  padding: '20px 24px',
}

function CustomTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const d = payload[0].payload
  return (
    <div style={{
      background: 'var(--c-surface)', border: '1px solid var(--c-border)',
      borderRadius: 8, padding: '10px 14px', fontSize: 12,
      boxShadow: '0 4px 16px var(--c-shadow)',
    }}>
      <div style={{ fontWeight: 600, color: 'var(--c-text)', marginBottom: 6 }}>Run #{d.label}</div>
      <div style={{ color: 'var(--c-muted)', marginBottom: 2 }}>Duration: <strong style={{ color: 'var(--c-text)' }}>{d.durationLabel}</strong></div>
      <div style={{ color: 'var(--c-muted)', textTransform: 'capitalize' }}>State: <strong style={{ color: 'var(--c-text)' }}>{d.state}</strong></div>
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
      {/* Card header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 20 }}>
        <div style={{
          width: 30, height: 30, borderRadius: 8,
          background: 'rgba(26,115,232,0.10)',
          display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
        }}>
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="#1a73e8" strokeWidth="2.5" strokeLinecap="round">
            <line x1="18" y1="20" x2="18" y2="10"/>
            <line x1="12" y1="20" x2="12" y2="4"/>
            <line x1="6"  y1="20" x2="6"  y2="14"/>
          </svg>
        </div>
        <div>
          <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--c-text)', lineHeight: 1.2 }}>Run Duration</div>
          <div style={{ fontSize: 11.5, color: 'var(--c-muted)', marginTop: 1 }}>Last 10 runs</div>
        </div>
      </div>

      {data.length === 0 ? (
        <div style={{ height: 200, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--c-muted)', fontSize: 13 }}>
          No run data
        </div>
      ) : (
        <ResponsiveContainer width="100%" height={210}>
          <BarChart data={data} margin={{ top: 4, right: 4, left: -16, bottom: 0 }} barCategoryGap="32%">
            <CartesianGrid strokeDasharray="3 3" stroke="var(--c-border)" vertical={false} />
            <XAxis
              dataKey="label"
              tick={{ fontSize: 11, fill: 'var(--c-muted)' }}
              axisLine={false}
              tickLine={false}
              label={{ value: 'Run #', position: 'insideBottom', offset: -2, fontSize: 11, fill: 'var(--c-muted)' }}
            />
            <YAxis
              tick={{ fontSize: 11, fill: 'var(--c-muted)' }}
              axisLine={false}
              tickLine={false}
              unit=" m"
            />
            <Tooltip content={<CustomTooltip />} cursor={{ fill: 'var(--c-hover)' }} />
            <Bar dataKey="mins" radius={[5, 5, 0, 0]}>
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
