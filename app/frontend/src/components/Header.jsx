import { formatDate } from '../utils.js'

function RefreshIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="23 4 23 10 17 10" />
      <polyline points="1 20 1 14 7 14" />
      <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
    </svg>
  )
}

function SunIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="5" />
      <line x1="12" y1="1" x2="12" y2="3" /><line x1="12" y1="21" x2="12" y2="23" />
      <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" /><line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
      <line x1="1" y1="12" x2="3" y2="12" /><line x1="21" y1="12" x2="23" y2="12" />
      <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" /><line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
    </svg>
  )
}

function MoonIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
    </svg>
  )
}

const iconBtn = {
  display: 'flex', alignItems: 'center', justifyContent: 'center',
  width: 36, height: 36, border: 'none', borderRadius: '50%',
  background: 'transparent', color: 'var(--c-muted)',
  cursor: 'pointer', transition: 'background 0.15s',
}

export default function Header({ lastRefreshed, onRefresh, dark, onToggleDark }) {
  return (
    <header style={{
      background: 'var(--c-surface)',
      borderBottom: '1px solid var(--c-border)',
      padding: '0 24px',
      height: 56,
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between',
      position: 'sticky',
      top: 0,
      zIndex: 10,
    }}>
      <span style={{ fontSize: 18, fontWeight: 500, color: '#1a73e8', letterSpacing: '-0.01em' }}>
        Social Media Pipeline
      </span>

      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        {lastRefreshed && (
          <span style={{ fontSize: 12, color: 'var(--c-muted)', fontWeight: 400 }}>
            Updated {formatDate(lastRefreshed.toISOString())}
          </span>
        )}
        <button
          onClick={onToggleDark}
          title={dark ? 'Switch to light mode' : 'Switch to dark mode'}
          style={iconBtn}
          onMouseEnter={e => e.currentTarget.style.background = 'var(--c-hover)'}
          onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
        >
          {dark ? <SunIcon /> : <MoonIcon />}
        </button>
        <button
          onClick={onRefresh}
          title="Refresh"
          style={iconBtn}
          onMouseEnter={e => e.currentTarget.style.background = 'var(--c-hover)'}
          onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
        >
          <RefreshIcon />
        </button>
      </div>
    </header>
  )
}
