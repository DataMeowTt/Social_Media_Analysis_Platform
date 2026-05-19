import { getStateColor, formatDuration } from '../utils.js'

const DISPLAY_NAME = {
  ingestion_tweets:            'Ingestion',
  delete_raw_duplicates:       'Dedup Raw',
  processing_silver:           'Processing',
  delete_processed_duplicates: 'Dedup Silver',
  analytics_gold:              'Analytics',
  analytics_sentiment:         'Sentiment',
  analytics_stance:            'Stance',
}

const STATE_ICON = {
  success:         '✓',
  running:         '↻',
  failed:          '✕',
  upstream_failed: '✕',
  queued:          '…',
  scheduled:       '⏱',
  none:            '—',
}

function ArrowConnector({ prevState, nextState }) {
  const flowing = prevState === 'success' && nextState === 'running'
  const done    = prevState === 'success' && nextState === 'success'
  const failed  = nextState === 'failed' || nextState === 'upstream_failed'

  const color = done ? '#34A853' : failed ? '#EA4335' : flowing ? '#1a73e8' : 'var(--c-border)'

  return (
    <div style={{ flexShrink: 0, width: 44, display: 'flex', alignItems: 'center', gap: 0 }}>
      <div style={{ position: 'relative', flex: 1, height: 2, background: color, borderRadius: 1, overflow: 'hidden' }}>
        {flowing && <div className="flow-pulse" />}
      </div>
      <svg width="9" height="14" viewBox="0 0 9 14" fill="none" style={{ flexShrink: 0 }}>
        <path d="M1 1L8 7L1 13" stroke={color} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
      </svg>
    </div>
  )
}

function TaskCard({ task }) {
  const state  = task?.state ?? 'none'
  const colors = getStateColor(state)
  const taskId = task?.task_id ?? ''

  return (
    <div style={{
      flex: 1,
      background: 'var(--c-surface)',
      border: '1px solid var(--c-border)',
      borderTop: `3px solid ${colors.border}`,
      borderRadius: 10,
      boxShadow: '0 2px 8px var(--c-shadow)',
      padding: '14px 16px',
      minWidth: 0,
    }}>
      <div style={{
        fontSize: 11, fontWeight: 700,
        color: 'var(--c-muted)',
        textTransform: 'uppercase', letterSpacing: '0.07em',
        marginBottom: 10,
      }}>
        {DISPLAY_NAME[taskId] ?? taskId}
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: 7, marginBottom: 10 }}>
        <span
          className={state === 'running' ? 'spin' : undefined}
          style={{ fontSize: 14, color: colors.border, fontWeight: 700, lineHeight: 1, flexShrink: 0 }}
        >
          {STATE_ICON[state] ?? '—'}
        </span>
        <span style={{
          fontSize: 11, fontWeight: 600,
          color: colors.text,
          background: colors.bg,
          borderRadius: 10, padding: '2px 9px',
          textTransform: 'capitalize',
          border: `1px solid ${colors.border}28`,
          whiteSpace: 'nowrap',
        }}>
          {state.replace(/_/g, ' ')}
        </span>
      </div>

      <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--c-text)', letterSpacing: '-0.01em' }}>
        {formatDuration(task?.duration_seconds)}
      </div>
    </div>
  )
}

export default function TaskFlow({ tasks }) {
  if (!tasks?.length) return null
  return (
    <div style={{ display: 'flex', alignItems: 'stretch', gap: 0 }}>
      {tasks.map((task, i) => (
        <div key={task.task_id} style={{ display: 'flex', alignItems: 'center', flex: 1, minWidth: 0 }}>
          <TaskCard task={task} />
          {i < tasks.length - 1 && (
            <ArrowConnector
              prevState={task.state}
              nextState={tasks[i + 1]?.state}
            />
          )}
        </div>
      ))}
    </div>
  )
}
