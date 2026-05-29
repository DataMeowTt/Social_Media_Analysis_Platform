import { useEffect, useState } from 'react'
import { apiFetch } from '../utils.js'

const PROFANITY_RE = new RegExp(
  '(đcm|dcm|đkm|dkm|đm\\b|\\bdm\\b|clm|vl\\b|\\bcc\\b|lồn|lon|cặc|cac|địt|\\bdit\\b|đéo|\\bdeo\\b|mẹ kiếp|đụ|du má)',
  'gi'
)

function CensoredText({ text, style }) {
  if (!text) return <span style={style}>(Không có nội dung)</span>
  const parts = []
  let last = 0, match
  const re = new RegExp(PROFANITY_RE.source, 'gi')
  while ((match = re.exec(text)) !== null) {
    if (match.index > last) parts.push({ t: text.slice(last, match.index), blur: false })
    parts.push({ t: match[0], blur: true })
    last = match.index + match[0].length
  }
  if (last < text.length) parts.push({ t: text.slice(last), blur: false })
  return (
    <span style={style}>
      {parts.map((p, i) =>
        p.blur
          ? <span key={i} style={{ filter: 'blur(4px)', userSelect: 'none', opacity: 0.7 }}>{p.t}</span>
          : <span key={i}>{p.t}</span>
      )}
    </span>
  )
}

const SENT_COLOR = {
  positive: '#22c55e',
  negative: '#ef4444',
  neutral:  '#94a3b8',
  mixed:    '#f59e0b',
}

function SentimentBadge({ label }) {
  const color = SENT_COLOR[label] ?? '#94a3b8'
  return (
    <span style={{
      background: color + '20', color,
      borderRadius: 6, padding: '2px 10px',
      fontSize: 11, fontWeight: 700, textTransform: 'capitalize',
    }}>
      {label ?? '—'}
    </span>
  )
}

function StatBar({ agree, disagree, total }) {
  const agreePct  = total > 0 ? Math.round((agree  / total) * 100) : 0
  const disagreePct = total > 0 ? Math.round((disagree / total) * 100) : 0
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      {/* Agree bar */}
      <div>
        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: 'var(--c-muted)', marginBottom: 4 }}>
          <span>Đồng ý</span>
          <span style={{ fontWeight: 600, color: '#22c55e' }}>{agreePct}%</span>
        </div>
        <div style={{ height: 6, borderRadius: 4, background: 'var(--c-hover)', overflow: 'hidden' }}>
          <div style={{ width: `${agreePct}%`, height: '100%', background: '#22c55e', borderRadius: 4, transition: 'width 0.4s' }} />
        </div>
      </div>
      {/* Disagree bar */}
      <div>
        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: 'var(--c-muted)', marginBottom: 4 }}>
          <span>Phản biện</span>
          <span style={{ fontWeight: 600, color: '#ef4444' }}>{disagreePct}%</span>
        </div>
        <div style={{ height: 6, borderRadius: 4, background: 'var(--c-hover)', overflow: 'hidden' }}>
          <div style={{ width: `${disagreePct}%`, height: '100%', background: '#ef4444', borderRadius: 4, transition: 'width 0.4s' }} />
        </div>
      </div>
    </div>
  )
}

function PostCard({ post, rank }) {
  const agree    = post.agree_count    ?? 0
  const disagree = post.disagree_count ?? 0
  const total    = post.total_comments ?? 0

  return (
    <div style={{
      flex: 1, minWidth: 0,
      background: 'var(--c-surface)',
      border: '1px solid var(--c-border)',
      borderRadius: 12,
      padding: '20px 22px',
      display: 'flex', flexDirection: 'column', gap: 16,
      boxShadow: '0 2px 8px var(--c-shadow)',
    }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8, flexWrap: 'wrap' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{
            width: 22, height: 22, borderRadius: 6,
            background: 'var(--c-hover)',
            fontSize: 11, fontWeight: 700, color: 'var(--c-muted)',
            display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
          }}>
            #{rank}
          </span>
          <SentimentBadge label={post.sentiment_label} />
        </div>
        <span style={{ fontSize: 11, color: 'var(--c-muted)', fontWeight: 500 }}>
          {post.page_name ?? ''}
        </span>
      </div>

      {/* Text preview */}
      <CensoredText
        text={post.text_preview}
        style={{
          display: '-webkit-box', WebkitLineClamp: 4,
          WebkitBoxOrient: 'vertical', overflow: 'hidden',
          fontSize: 13, lineHeight: 1.65, color: 'var(--c-text)',
          borderLeft: '3px solid var(--c-border)',
          paddingLeft: 12,
        }}
      />

      {/* Agree/Disagree bars */}
      <StatBar agree={agree} disagree={disagree} total={total} />

      {/* Stats row */}
      <div style={{
        display: 'flex', gap: 16,
        paddingTop: 12, borderTop: '1px solid var(--c-border)',
      }}>
        {[
          { label: 'Đồng ý',    value: agree,    color: '#22c55e' },
          { label: 'Phản biện', value: disagree,  color: '#ef4444' },
          { label: 'Tổng',      value: total,     color: 'var(--c-muted)' },
        ].map(({ label, value, color }) => (
          <div key={label} style={{ flex: 1, textAlign: 'center' }}>
            <div style={{ fontSize: 20, fontWeight: 800, color, lineHeight: 1 }}>
              {value.toLocaleString()}
            </div>
            <div style={{ fontSize: 10.5, color: 'var(--c-muted)', marginTop: 3 }}>{label}</div>
          </div>
        ))}
      </div>
    </div>
  )
}

export default function FacebookCharts() {
  const [posts, setPosts] = useState(null)

  useEffect(() => {
    apiFetch('/api/facebook/controversial-posts')
      .then(r => r.json())
      .then(setPosts)
      .catch(() => setPosts([]))
  }, [])

  return (
    <div style={{
      background: 'var(--c-surface)',
      border: '1px solid var(--c-border)',
      borderRadius: 12,
      padding: '20px 24px',
      boxShadow: '0 2px 8px var(--c-shadow)',
    }}>
      {posts === null ? (
        <div style={{ textAlign: 'center', padding: '36px 0', color: 'var(--c-muted)', fontSize: 13 }}>
          Đang tải...
        </div>
      ) : posts.length === 0 ? (
        <div style={{ textAlign: 'center', padding: '36px 0', color: 'var(--c-muted)', fontSize: 13 }}>
          Chưa có dữ liệu stance
        </div>
      ) : (
        <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
          {posts.map((post, i) => (
            <PostCard key={i} post={post} rank={i + 1} />
          ))}
        </div>
      )}
    </div>
  )
}
