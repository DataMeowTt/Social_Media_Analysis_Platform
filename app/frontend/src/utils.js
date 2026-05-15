// State → color mapping (Google palette)
export const STATE_COLORS = {
  success:         { bg: '#e6f4ea', text: '#137333', border: '#34A853' },
  running:         { bg: '#e8f0fe', text: '#1a73e8', border: '#1a73e8' },
  failed:          { bg: '#fce8e6', text: '#c5221f', border: '#EA4335' },
  upstream_failed: { bg: '#fce8e6', text: '#c5221f', border: '#EA4335' },
  queued:          { bg: '#fef7e0', text: '#b06000', border: '#F9AB00' },
  scheduled:       { bg: '#fef7e0', text: '#b06000', border: '#F9AB00' },
  none:            { bg: '#f1f3f4', text: '#5f6368', border: '#dadce0' },
}

export function getStateColor(state) {
  return STATE_COLORS[state] ?? STATE_COLORS.none
}

// Bar chart fill color by state
export const STATE_BAR_FILL = {
  success: '#34A853',
  failed:  '#EA4335',
  running: '#1a73e8',
}

export function getBarFill(state) {
  return STATE_BAR_FILL[state] ?? '#9aa0a6'
}

export function formatDuration(seconds) {
  if (seconds == null) return '—'
  const m = Math.floor(seconds / 60)
  const s = Math.round(seconds % 60)
  if (m === 0) return `${s}s`
  return `${m}m ${s}s`
}

export function formatDate(isoString) {
  if (!isoString) return '—'
  return new Date(isoString).toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  })
}
