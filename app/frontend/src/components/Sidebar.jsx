import { formatDate } from '../utils.js'
import iconStatistic from '../../image/icon_statistic.png'
import logoUet from '../../image/logo_uet.webp'

const PLATFORMS = [
  { id: 'twitter',  label: 'Twitter',  color: '#1DA1F2', hasPipeline: true },
  { id: 'youtube',  label: 'YouTube',  color: '#FF0000', hasPipeline: true },
  { id: 'facebook', label: 'Facebook', color: '#1877F2', hasPipeline: true },
]

function TwitterIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
      <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-4.714-6.231-5.401 6.231H2.744l7.73-8.835L1.254 2.25H8.08l4.259 5.631 5.905-5.631zm-1.161 17.52h1.833L7.084 4.126H5.117z"/>
    </svg>
  )
}

function YouTubeIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
      <path d="M23.498 6.186a3.016 3.016 0 0 0-2.122-2.136C19.505 3.545 12 3.545 12 3.545s-7.505 0-9.377.505A3.017 3.017 0 0 0 .502 6.186C0 8.07 0 12 0 12s0 3.93.502 5.814a3.016 3.016 0 0 0 2.122 2.136c1.871.505 9.376.505 9.376.505s7.505 0 9.377-.505a3.015 3.015 0 0 0 2.122-2.136C24 15.93 24 12 24 12s0-3.93-.502-5.814zM9.545 15.568V8.432L15.818 12l-6.273 3.568z"/>
    </svg>
  )
}

function FacebookIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
      <path d="M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z"/>
    </svg>
  )
}

function RefreshIcon() {
  return (
    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="23 4 23 10 17 10" />
      <polyline points="1 20 1 14 7 14" />
      <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
    </svg>
  )
}

function SunIcon() {
  return (
    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
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
    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
    </svg>
  )
}

function LogoutIcon() {
  return (
    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
      <polyline points="16 17 21 12 16 7" />
      <line x1="21" y1="12" x2="9" y2="12" />
    </svg>
  )
}

const ICONS = {
  twitter:  <TwitterIcon />,
  youtube:  <YouTubeIcon />,
  facebook: <FacebookIcon />,
}

function NavAction({ icon, title, onClick }) {
  return (
    <button
      onClick={onClick}
      title={title}
      style={{
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        width: 34, height: 34, borderRadius: 8,
        background: 'transparent', border: 'none',
        color: 'var(--c-nav-muted)', cursor: 'pointer',
        transition: 'background 0.15s, color 0.15s',
      }}
      onMouseEnter={e => {
        e.currentTarget.style.background = 'var(--c-nav-hover)'
        e.currentTarget.style.color = 'var(--c-nav-text)'
      }}
      onMouseLeave={e => {
        e.currentTarget.style.background = 'transparent'
        e.currentTarget.style.color = 'var(--c-nav-muted)'
      }}
    >
      {icon}
    </button>
  )
}

export { PLATFORMS }

export default function Sidebar({ selectedPlatform, onChange, dark, onToggleDark, onLogout, lastRefreshed, onRefresh }) {
  return (
    <aside style={{
      width: 220,
      minWidth: 220,
      background: 'var(--c-nav)',
      height: '100vh',
      position: 'sticky',
      top: 0,
      display: 'flex',
      flexDirection: 'column',
      borderRight: '1px solid var(--c-nav-border)',
      flexShrink: 0,
    }}>

      {/* Brand */}
      <div style={{
        padding: '20px 16px 18px',
        borderBottom: '1px solid var(--c-nav-border)',
        display: 'flex',
        alignItems: 'center',
        gap: 11,
        flexShrink: 0,
      }}>
        <img src={iconStatistic} alt="icon" style={{ width: 36, height: 36, flexShrink: 0 }} />
        <div>
          <div style={{ fontSize: 13.5, fontWeight: 700, color: 'var(--c-nav-text)', letterSpacing: '-0.01em', lineHeight: 1.3 }}>
            Analytics
          </div>
          <div style={{ fontSize: 10.5, color: 'var(--c-nav-muted)', letterSpacing: '0.03em' }}>
            Social Media
          </div>
        </div>
      </div>

      {/* Platform nav */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '14px 8px' }}>
        <div style={{
          fontSize: 10, fontWeight: 700,
          color: 'var(--c-nav-muted)',
          textTransform: 'uppercase', letterSpacing: '0.12em',
          padding: '0 8px 10px',
        }}>
          Platforms
        </div>

        {PLATFORMS.map(p => {
          const active = p.id === selectedPlatform && !p.disabled
          return (
            <button
              key={p.id}
              disabled={p.disabled}
              onClick={() => !p.disabled && onChange(p.id)}
              style={{
                width: '100%',
                display: 'flex', alignItems: 'center', gap: 10,
                padding: '9px 10px 9px 12px',
                marginBottom: 2,
                borderRadius: 8,
                border: 'none',
                background: active ? 'rgba(255,255,255,0.09)' : 'transparent',
                color: active ? 'var(--c-nav-text)' : 'var(--c-nav-muted)',
                cursor: p.disabled ? 'default' : 'pointer',
                fontSize: 13.5,
                fontWeight: active ? 600 : 400,
                fontFamily: 'inherit',
                textAlign: 'left',
                opacity: p.disabled ? 0.38 : 1,
                transition: 'background 0.15s, color 0.15s',
                position: 'relative',
              }}
              onMouseEnter={e => {
                if (!p.disabled && !active) e.currentTarget.style.background = 'var(--c-nav-hover)'
              }}
              onMouseLeave={e => {
                if (!p.disabled && !active) e.currentTarget.style.background = 'transparent'
              }}
            >
              {/* Active left bar */}
              {active && (
                <div style={{
                  position: 'absolute', left: 0, top: '50%', transform: 'translateY(-50%)',
                  width: 3, height: 22, borderRadius: '0 3px 3px 0',
                  background: p.color,
                }} />
              )}

              <span style={{ color: active ? p.color : 'var(--c-nav-muted)', display: 'flex', flexShrink: 0, transition: 'color 0.15s' }}>
                {ICONS[p.id]}
              </span>
              <span style={{ flex: 1 }}>{p.label}</span>

              {active && (
                <div style={{ width: 6, height: 6, borderRadius: '50%', background: p.color, flexShrink: 0, opacity: 0.8 }} />
              )}
              {p.disabled && (
                <span style={{
                  fontSize: 9, fontWeight: 700, padding: '2px 6px',
                  background: 'rgba(255,255,255,0.07)', borderRadius: 4,
                  color: 'var(--c-nav-muted)', textTransform: 'uppercase', letterSpacing: '0.05em',
                }}>
                  Soon
                </span>
              )}
            </button>
          )
        })}
      </div>

      {/* Bottom actions */}
      <div style={{
        padding: '12px 14px 16px',
        borderTop: '1px solid var(--c-nav-border)',
        flexShrink: 0,
      }}>
        {lastRefreshed && (
          <div style={{ fontSize: 10.5, color: 'var(--c-nav-muted)', textAlign: 'center', marginBottom: 10 }}>
            Updated {formatDate(lastRefreshed.toISOString())}
          </div>
        )}
        <div style={{ display: 'flex', gap: 4, justifyContent: 'center' }}>
          <NavAction icon={<RefreshIcon />} title="Refresh" onClick={onRefresh} />
          <NavAction
            icon={dark ? <SunIcon /> : <MoonIcon />}
            title={dark ? 'Light mode' : 'Dark mode'}
            onClick={onToggleDark}
          />
          <NavAction icon={<LogoutIcon />} title="Logout" onClick={onLogout} />
        </div>
      </div>

      {/* UET Logo */}
      <div style={{
        padding: '10px 16px 14px',
        borderTop: '1px solid var(--c-nav-border)',
        display: 'flex', alignItems: 'center', gap: 9,
        flexShrink: 0, opacity: 0.65,
      }}>
        <img src={logoUet} alt="UET logo" style={{ width: 28, height: 28, flexShrink: 0 }} />
        <div style={{ fontSize: 10, color: 'var(--c-nav-muted)', lineHeight: 1.4 }}>
          <div style={{ fontWeight: 600 }}>UET — VNU</div>
          <div>Đại học Công nghệ</div>
        </div>
      </div>
    </aside>
  )
}
