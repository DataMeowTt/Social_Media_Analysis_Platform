import { useState, useEffect, useCallback, useRef } from 'react'
import { apiFetch } from '../utils.js'

const DISPLAY_NAME = {
  ingestion_tweets:            'Ingestion',
  ingestion_comments:          'Ingestion',
  ingestion_posts:             'Ingestion',
  delete_raw_duplicates:       'Dedup Raw',
  processing_silver:           'Processing',
  delete_processed_duplicates: 'Dedup Silver',
  analytics_gold:              'Analytics',
  analytics_sentiment:         'Sentiment',
  analytics_stance:            'Stance',
  build_conversation_threads:  'Threads',
  gemini_analysis_and_save:    'Gemini',
  gemini_analysis:             'Gemini',
  stance_analysis:             'Stance',
}

function lineColor(line) {
  if (/ERROR|CRITICAL/i.test(line))   return '#f28b82'
  if (/WARNING|WARN/i.test(line))     return '#fdd663'
  if (/\bINFO\b/i.test(line))        return '#8ab4f8'
  return '#e8eaed'
}

export default function LogDrawer({ dagId, runId, task, onClose }) {
  const [content, setContent]   = useState('')
  const [loading, setLoading]   = useState(true)
  const [error, setError]       = useState(null)
  const [autoScroll, setAutoScroll] = useState(true)
  const bottomRef = useRef(null)
  const scrollRef = useRef(null)

  const isRunning = task?.state === 'running'
  const taskLabel = DISPLAY_NAME[task?.task_id] ?? task?.task_id ?? ''

  const fetchLogs = useCallback(async () => {
    if (!runId || !task?.task_id) return
    const tryNum = Math.max(1, task?.try_number ?? 1)
    const params = new URLSearchParams({ run_id: runId, try_number: tryNum })
    try {
      const res = await apiFetch(`/api/logs/${dagId}/${task.task_id}?${params}`)
      if (res.ok) {
        const data = await res.json()
        setContent(data.content ?? '')
        setError(null)
      } else {
        setError('Failed to load logs')
      }
    } catch {
      setError('Network error')
    } finally {
      setLoading(false)
    }
  }, [dagId, runId, task?.task_id, task?.try_number])

  useEffect(() => {
    setContent('')
    setLoading(true)
    setError(null)
    fetchLogs()
  }, [fetchLogs])

  useEffect(() => {
    if (!isRunning) return
    const id = setInterval(fetchLogs, 3000)
    return () => clearInterval(id)
  }, [fetchLogs, isRunning])

  useEffect(() => {
    if (autoScroll) bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [content, autoScroll])

  const handleScroll = () => {
    const el = scrollRef.current
    if (!el) return
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40
    setAutoScroll(atBottom)
  }

  const lines = content.split('\n')

  return (
    <div style={{
      position: 'fixed', bottom: 0, left: 220, right: 0, zIndex: 50,
      height: 360,
      background: '#1a1b1e',
      borderTop: '1px solid #3c4043',
      display: 'flex', flexDirection: 'column',
      boxShadow: '0 -4px 24px rgba(0,0,0,0.4)',
    }}>

      {/* Header */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '0 16px', height: 40, flexShrink: 0,
        borderBottom: '1px solid #3c4043',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ fontSize: 12, fontWeight: 700, color: '#e8eaed', letterSpacing: '0.05em', textTransform: 'uppercase' }}>
            Logs
          </span>
          <span style={{ fontSize: 11, color: '#9aa0a6' }}>
            {dagId} / <span style={{ color: '#8ab4f8' }}>{taskLabel}</span>
          </span>
          {isRunning && (
            <span style={{
              fontSize: 10, fontWeight: 600, color: '#1a73e8',
              background: '#1a73e820', border: '1px solid #1a73e840',
              borderRadius: 6, padding: '1px 7px', letterSpacing: '0.05em',
            }}>
              LIVE
            </span>
          )}
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {!autoScroll && (
            <button
              onClick={() => { setAutoScroll(true); bottomRef.current?.scrollIntoView({ behavior: 'smooth' }) }}
              style={{
                fontSize: 11, color: '#9aa0a6', background: 'transparent',
                border: '1px solid #3c4043', borderRadius: 5,
                padding: '2px 8px', cursor: 'pointer', fontFamily: 'inherit',
              }}
            >
              ↓ Jump to bottom
            </button>
          )}
          <button
            onClick={onClose}
            style={{
              width: 24, height: 24, borderRadius: 5,
              background: 'transparent', border: 'none',
              color: '#9aa0a6', cursor: 'pointer', fontSize: 16, lineHeight: 1,
              display: 'flex', alignItems: 'center', justifyContent: 'center',
            }}
          >
            ✕
          </button>
        </div>
      </div>

      {/* Log content */}
      <div
        ref={scrollRef}
        onScroll={handleScroll}
        style={{
          flex: 1, overflow: 'auto', padding: '10px 16px',
          fontFamily: "'JetBrains Mono', 'Fira Code', 'Consolas', monospace",
          fontSize: 11.5, lineHeight: 1.7,
        }}
      >
        {loading && (
          <span style={{ color: '#9aa0a6' }}>Loading logs…</span>
        )}
        {error && (
          <span style={{ color: '#f28b82' }}>{error}</span>
        )}
        {!loading && !error && lines.map((line, i) => (
          <div key={i} style={{ color: lineColor(line), whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
            {line || ' '}
          </div>
        ))}
        <div ref={bottomRef} />
      </div>
    </div>
  )
}
