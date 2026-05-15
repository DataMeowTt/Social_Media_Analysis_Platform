import { getStateColor, formatDuration } from '../utils.js'

const DISPLAY_NAME = {
  ingestion_tweets:            'Ingestion',
  delete_raw_duplicates:       'Dedup Raw',
  processing_silver:           'Processing',
  delete_processed_duplicates: 'Dedup Silver',
  analytics_gold:              'Analytics',
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

function Connector({ prevState, nextState }) {
  const flowing = prevState === 'success' && nextState === 'running'
  const done    = prevState === 'success' && nextState === 'success'
  const failed  = nextState === 'failed' || nextState === 'upstream_failed'

  const lineColor  = done ? '#34A853' : failed ? '#EA4335' : '#dadce0'
  const arrowColor = done ? '#34A853' : flowing ? '#1a73e8' : failed ? '#EA4335' : '#9aa0a6'

  return (
    <div style={{
      flexShrink: 0,
      width: 36,
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      justifyContent: 'center',
      gap: 3,
    }}>
      {/* Connector line */}
      <div style={{
        position: 'relative',
        width: 28,
        height: 3,
        background: lineColor,
        borderRadius: 2,
        overflow: 'hidden',
      }}>
        {flowing && <div className="flow-pulse" />}
      </div>

      {/* Arrow */}
      <span style={{ fontSize: 13, color: arrowColor, lineHeight: 1, fontWeight: 600 }}>
        ›
      </span>
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
      borderLeft: `4px solid ${colors.border}`,
      borderRadius: 8,
      boxShadow: '0 1px 3px var(--c-shadow)',
      padding: '16px 18px',
      minWidth: 0,
    }}>
      <div style={{ fontSize: 13, fontWeight: 500, color: 'var(--c-muted)', marginBottom: 8 }}>
        {DISPLAY_NAME[taskId] ?? taskId}
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
        <span
          className={state === 'running' ? 'spin' : undefined}
          style={{ fontSize: 16, color: colors.border, fontWeight: 700, lineHeight: 1 }}
        >
          {STATE_ICON[state] ?? '—'}
        </span>
        <span style={{
          fontSize: 12,
          fontWeight: 500,
          color: colors.text,
          background: colors.bg,
          borderRadius: 12,
          padding: '2px 10px',
          textTransform: 'capitalize',
        }}>
          {state.replace(/_/g, ' ')}
        </span>
      </div>

      <div style={{ fontSize: 12, color: 'var(--c-muted)' }}>
        {formatDuration(task?.duration_seconds)}
      </div>
    </div>
  )
}

export default function TaskFlow({ tasks }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 0 }}>
      {(tasks ?? []).map((task, i) => (
        <div key={task.task_id} style={{ display: 'flex', alignItems: 'center', flex: 1, minWidth: 0 }}>
          <TaskCard task={task} />
          {i < tasks.length - 1 && (
            <Connector
              prevState={task.state}
              nextState={tasks[i + 1]?.state}
            />
          )}
        </div>
      ))}
    </div>
  )
}
