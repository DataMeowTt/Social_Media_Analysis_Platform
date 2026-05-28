import { useState, useEffect } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
} from 'recharts'
import { apiFetch } from '../utils.js'

const S_COLORS = {
  positive: '#22c55e',
  negative: '#ef4444',
  mixed:    '#f59e0b',
  neutral:  '#94a3b8',
}

const E_COLORS = {
  android: '#3ddc84',
  iphone:  '#a78bfa',
}

const CARD = {
  background: 'var(--c-surface)',
  border: '1px solid var(--c-border)',
  borderRadius: 12,
  padding: '20px 24px',
  boxShadow: '0 2px 8px var(--c-shadow)',
}

function entityColor(e) {
  return E_COLORS[e] ?? '#94a3b8'
}

function filterByVideo(data, videoId) {
  return videoId === '__all__' ? data : data.filter(r => r.video_id === videoId)
}

function buildEntityStats(data) {
  const map = {}
  for (const r of data) {
    if (!map[r.entity]) map[r.entity] = { total: 0, sents: {}, aspects: {} }
    const e = map[r.entity]
    e.total += r.mention_count
    e.sents[r.sentiment] = (e.sents[r.sentiment] || 0) + r.mention_count
    e.aspects[r.aspect] = (e.aspects[r.aspect] || 0) + r.mention_count
  }
  return map
}

function buildAspectData(data, entity) {
  const map = {}
  for (const r of data) {
    if (r.entity !== entity) continue
    if (!map[r.aspect]) map[r.aspect] = { aspect: r.aspect, positive: 0, negative: 0, mixed: 0, neutral: 0, total: 0 }
    map[r.aspect][r.sentiment] = (map[r.aspect][r.sentiment] || 0) + r.mention_count
    map[r.aspect].total += r.mention_count
  }
  return Object.values(map)
    .sort((a, b) => b.total - a.total)
    .slice(0, 10)
    .map(d => ({ ...d, aspect: d.aspect.replace(/_/g, ' ') }))
}

function buildTopThreads(data) {
  const map = {}
  for (const r of data) {
    if (!map[r.thread_id]) map[r.thread_id] = {
      thread_id: r.thread_id,
      video_id: r.video_id,
      brand_comment_count: r.brand_comment_count,
      rows: [],
    }
    map[r.thread_id].rows.push(r)
  }
  return Object.values(map)
    .sort((a, b) => b.brand_comment_count - a.brand_comment_count)
    .slice(0, 6)
}

// ─── Entity Scorecard ─────────────────────────────────────────────────────────

function EntityCard({ entity, stats, active, onClick }) {
  const color = entityColor(entity)
  const total = stats.total
  const sentOrder = ['positive', 'negative', 'mixed', 'neutral']
  const topAspect = Object.entries(stats.aspects).sort((a, b) => b[1] - a[1])[0]?.[0] ?? '—'

  return (
    <div
      onClick={onClick}
      style={{
        ...CARD,
        flex: 1,
        minWidth: 160,
        cursor: 'pointer',
        border: active ? `1.5px solid ${color}50` : '1px solid var(--c-border)',
        background: active ? color + '0a' : 'var(--c-surface)',
        transition: 'all 0.15s',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
        <div style={{ width: 10, height: 10, borderRadius: '50%', background: color, flexShrink: 0 }} />
        <span style={{ fontSize: 14, fontWeight: 700, color: 'var(--c-text)', textTransform: 'capitalize' }}>
          {entity}
        </span>
      </div>

      <div style={{ fontSize: 30, fontWeight: 800, color: 'var(--c-text)', lineHeight: 1, marginBottom: 2 }}>
        {total.toLocaleString()}
      </div>
      <div style={{ fontSize: 11, color: 'var(--c-muted)', marginBottom: 14 }}>total mentions</div>

      {/* Sentiment bar */}
      <div style={{ height: 7, borderRadius: 4, overflow: 'hidden', display: 'flex', marginBottom: 8, background: 'var(--c-hover)' }}>
        {sentOrder.map(s => {
          const count = stats.sents[s] || 0
          const pct = total > 0 ? (count / total) * 100 : 0
          if (pct < 0.5) return null
          return <div key={s} style={{ width: `${pct}%`, background: S_COLORS[s], transition: 'width 0.4s' }} />
        })}
      </div>

      {/* Sentiment percentages */}
      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 12 }}>
        {sentOrder.filter(s => stats.sents[s]).map(s => (
          <div key={s} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <div style={{ width: 7, height: 7, borderRadius: 2, background: S_COLORS[s] }} />
            <span style={{ fontSize: 10.5, color: 'var(--c-muted)' }}>
              {Math.round((stats.sents[s] / total) * 100)}%
            </span>
          </div>
        ))}
      </div>

      <div style={{ fontSize: 11, color: 'var(--c-muted)' }}>
        Top topic:{' '}
        <span style={{ color, fontWeight: 600, textTransform: 'capitalize' }}>
          {topAspect.replace(/_/g, ' ')}
        </span>
      </div>
    </div>
  )
}

// ─── Aspect Chart ─────────────────────────────────────────────────────────────

function AspectTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div style={{
      background: 'var(--c-surface)', border: '1px solid var(--c-border)',
      borderRadius: 8, padding: '10px 14px', fontSize: 12,
      boxShadow: '0 4px 16px var(--c-shadow)',
    }}>
      <div style={{ fontWeight: 600, marginBottom: 6, textTransform: 'capitalize', color: 'var(--c-text)' }}>
        {label}
      </div>
      {payload.map(p => p.value > 0 && (
        <div key={p.dataKey} style={{ color: S_COLORS[p.dataKey], marginBottom: 2 }}>
          {p.dataKey.charAt(0).toUpperCase() + p.dataKey.slice(1)}: <strong>{p.value}</strong>
        </div>
      ))}
    </div>
  )
}

function AspectChart({ data }) {
  const chartHeight = Math.max(220, data.length * 38)
  return (
    <ResponsiveContainer width="100%" height={chartHeight}>
      <BarChart
        data={data}
        layout="vertical"
        margin={{ top: 0, right: 16, left: 96, bottom: 0 }}
        barCategoryGap="28%"
      >
        <XAxis
          type="number"
          tick={{ fontSize: 11, fill: 'var(--c-muted)' }}
          axisLine={false} tickLine={false}
        />
        <YAxis
          type="category" dataKey="aspect"
          tick={{ fontSize: 12, fill: 'var(--c-text)', textTransform: 'capitalize' }}
          axisLine={false} tickLine={false} width={96}
        />
        <Tooltip content={<AspectTooltip />} cursor={{ fill: 'var(--c-hover)' }} />
        <Bar dataKey="positive" stackId="s" fill={S_COLORS.positive} />
        <Bar dataKey="mixed"    stackId="s" fill={S_COLORS.mixed} />
        <Bar dataKey="neutral"  stackId="s" fill={S_COLORS.neutral} />
        <Bar dataKey="negative" stackId="s" fill={S_COLORS.negative} radius={[0, 4, 4, 0]} />
      </BarChart>
    </ResponsiveContainer>
  )
}

// ─── Evidence Feed ────────────────────────────────────────────────────────────

function SentimentBadge({ sentiment }) {
  return (
    <span style={{
      background: S_COLORS[sentiment] + '20',
      color: S_COLORS[sentiment],
      borderRadius: 6, padding: '2px 8px',
      fontSize: 11, fontWeight: 600, textTransform: 'capitalize',
    }}>
      {sentiment}
    </span>
  )
}

function EntityBadge({ entity }) {
  const color = entityColor(entity)
  return (
    <span style={{
      background: color + '18', color,
      borderRadius: 6, padding: '2px 8px',
      fontSize: 11, fontWeight: 700, textTransform: 'capitalize',
    }}>
      {entity}
    </span>
  )
}

function ThreadCard({ thread }) {
  const topRow = [...thread.rows].sort((a, b) => b.mention_count - a.mention_count)[0]
  const rest = thread.rows.filter(r => r !== topRow).slice(0, 3)

  return (
    <div style={{ ...CARD, padding: '14px 18px' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap', marginBottom: 10 }}>
        <EntityBadge entity={topRow.entity} />
        <span style={{
          background: 'var(--c-hover)', color: 'var(--c-muted)',
          borderRadius: 6, padding: '2px 8px', fontSize: 11, textTransform: 'capitalize',
        }}>
          {topRow.aspect.replace(/_/g, ' ')}
        </span>
        <SentimentBadge sentiment={topRow.sentiment} />
        <span style={{
          background: topRow.intensity === 'strong' ? 'var(--c-hover)' : 'transparent',
          color: 'var(--c-muted)', fontSize: 10, borderRadius: 4, padding: '2px 6px',
          textTransform: 'capitalize',
        }}>
          {topRow.intensity}
        </span>
        <span style={{ marginLeft: 'auto', fontSize: 11, color: 'var(--c-muted)', whiteSpace: 'nowrap' }}>
          {thread.brand_comment_count} comments
        </span>
      </div>

      <div style={{
        fontSize: 13, color: 'var(--c-text)', fontStyle: 'italic', lineHeight: 1.6,
        borderLeft: `3px solid ${S_COLORS[topRow.sentiment]}`,
        paddingLeft: 12, marginBottom: rest.length ? 10 : 0,
      }}>
        "{topRow.evidence}"
      </div>

      {rest.length > 0 && (
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          {rest.map((r, i) => (
            <span key={i} style={{
              background: 'var(--c-hover)', color: 'var(--c-muted)',
              borderRadius: 20, padding: '2px 9px', fontSize: 10.5, textTransform: 'capitalize',
            }}>
              {r.entity} · {r.aspect.replace(/_/g, ' ')} ·{' '}
              <span style={{ color: S_COLORS[r.sentiment] }}>{r.sentiment}</span>
            </span>
          ))}
        </div>
      )}
    </div>
  )
}

// ─── Main Component ───────────────────────────────────────────────────────────

export default function ThreadInsights() {
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [videoId, setVideoId] = useState('__all__')
  const [activeEntity, setActiveEntity] = useState(null)

  useEffect(() => {
    apiFetch('/api/youtube/thread-insights')
      .then(r => r.json())
      .then(d => { setData(d); setActiveEntity(null) })
      .catch(err => setError(err.message))
  }, [])

  if (error) return (
    <div style={{ ...CARD, textAlign: 'center', padding: 48, color: 'var(--c-muted)' }}>
      Failed to load thread insights: {error}
    </div>
  )

  if (!data) return (
    <div style={{ ...CARD, textAlign: 'center', padding: 48 }}>
      <div style={{ fontSize: 13, color: 'var(--c-muted)' }}>Loading insights...</div>
    </div>
  )

  if (!data.length) return (
    <div style={{ ...CARD, textAlign: 'center', padding: 48, color: 'var(--c-muted)' }}>
      No thread insights available yet.
    </div>
  )

  const videoIds = [...new Set(data.map(r => r.video_id))]
  const filtered = filterByVideo(data, videoId)
  const entityStats = buildEntityStats(filtered)
  const entities = Object.keys(entityStats).sort((a, b) => entityStats[b].total - entityStats[a].total)
  const currentEntity = activeEntity ?? entities[0]
  const aspectData = buildAspectData(filtered, currentEntity)
  const threads = buildTopThreads(filtered)
  const totalMentions = Object.values(entityStats).reduce((s, e) => s + e.total, 0)
  const totalThreads = new Set(filtered.map(r => r.thread_id)).size

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>

      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', flexWrap: 'wrap', gap: 12 }}>
        <div>
          <div style={{ fontSize: 16, fontWeight: 700, color: 'var(--c-text)' }}>Thread Insights</div>
          <div style={{ fontSize: 12, color: 'var(--c-muted)', marginTop: 3 }}>
            {totalMentions.toLocaleString()} mentions across {totalThreads} conversations
            {videoIds.length > 1 ? ` · ${videoIds.length} videos` : ''}
          </div>
        </div>

        {/* Video filter — only shown when multiple videos */}
        {videoIds.length > 1 && (
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
            {['__all__', ...videoIds].map(v => (
              <button
                key={v}
                onClick={() => { setVideoId(v); setActiveEntity(null) }}
                style={{
                  background: videoId === v ? 'var(--c-text)' : 'var(--c-surface)',
                  color: videoId === v ? 'var(--c-app)' : 'var(--c-muted)',
                  border: '1px solid var(--c-border)', borderRadius: 20,
                  padding: '4px 14px', fontSize: 11, cursor: 'pointer', fontFamily: 'inherit',
                  transition: 'all 0.15s',
                }}
              >
                {v === '__all__' ? 'All videos' : v}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Entity scorecards */}
      <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap' }}>
        {entities.map(e => (
          <EntityCard
            key={e}
            entity={e}
            stats={entityStats[e]}
            active={e === currentEntity}
            onClick={() => setActiveEntity(e)}
          />
        ))}
      </div>

      {/* Aspect breakdown */}
      <div style={CARD}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 10, marginBottom: 16 }}>
          <div>
            <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--c-text)' }}>Aspect Breakdown</div>
            <div style={{ fontSize: 11, color: 'var(--c-muted)', marginTop: 2 }}>
              Mention volume by topic and sentiment —{' '}
              <span style={{ color: entityColor(currentEntity), fontWeight: 600, textTransform: 'capitalize' }}>
                {currentEntity}
              </span>
            </div>
          </div>

          {/* Entity tabs */}
          <div style={{ display: 'flex', gap: 4 }}>
            {entities.map(e => {
              const active = e === currentEntity
              const color = entityColor(e)
              return (
                <button
                  key={e}
                  onClick={() => setActiveEntity(e)}
                  style={{
                    background: active ? color + '18' : 'transparent',
                    border: `1px solid ${active ? color + '60' : 'transparent'}`,
                    borderRadius: 8, padding: '5px 14px', fontSize: 12,
                    fontWeight: active ? 600 : 400,
                    color: active ? color : 'var(--c-muted)',
                    cursor: 'pointer', fontFamily: 'inherit', textTransform: 'capitalize',
                    transition: 'all 0.15s',
                  }}
                >
                  {e}
                </button>
              )
            })}
          </div>
        </div>

        {/* Legend */}
        <div style={{ display: 'flex', gap: 14, marginBottom: 16 }}>
          {['positive', 'negative', 'mixed', 'neutral'].map(s => (
            <div key={s} style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
              <div style={{ width: 10, height: 10, borderRadius: 2, background: S_COLORS[s] }} />
              <span style={{ fontSize: 11, color: 'var(--c-muted)', textTransform: 'capitalize' }}>{s}</span>
            </div>
          ))}
        </div>

        {aspectData.length === 0 ? (
          <div style={{ textAlign: 'center', padding: '32px 0', color: 'var(--c-muted)', fontSize: 13 }}>
            No data for this entity
          </div>
        ) : (
          <AspectChart data={aspectData} />
        )}
      </div>

      {/* Top conversations */}
      <div>
        <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--c-text)', marginBottom: 12 }}>
          Top Conversations
          <span style={{ fontSize: 11, fontWeight: 400, color: 'var(--c-muted)', marginLeft: 8 }}>
            by thread size
          </span>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(360px, 1fr))', gap: 12 }}>
          {threads.map(t => <ThreadCard key={t.thread_id} thread={t} />)}
        </div>
      </div>

    </div>
  )
}
