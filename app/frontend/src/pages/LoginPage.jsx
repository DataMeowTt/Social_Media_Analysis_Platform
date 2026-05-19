import { useState } from 'react'

const UserIcon = () => (
  <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
    <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/>
    <circle cx="12" cy="7" r="4"/>
  </svg>
)

const LockIcon = () => (
  <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3" y="11" width="18" height="11" rx="2"/>
    <path d="M7 11V7a5 5 0 0 1 10 0v4"/>
  </svg>
)

const EyeIcon = ({ open }) => open ? (
  <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
    <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/>
    <circle cx="12" cy="12" r="3"/>
  </svg>
) : (
  <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
    <path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/>
    <line x1="1" y1="1" x2="23" y2="23"/>
  </svg>
)

const AlertIcon = () => (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
    <circle cx="12" cy="12" r="10"/>
    <line x1="12" y1="8" x2="12" y2="12"/>
    <line x1="12" y1="16" x2="12.01" y2="16"/>
  </svg>
)

export default function LoginPage({ onLogin }) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [showPass, setShowPass] = useState(false)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const [focused, setFocused] = useState(null)

  const handleSubmit = async (e) => {
    e.preventDefault()
    setLoading(true)
    setError('')
    try {
      const res = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password }),
      })
      if (!res.ok) {
        setError('Sai tên đăng nhập hoặc mật khẩu.')
        return
      }
      const { token } = await res.json()
      localStorage.setItem('authToken', token)
      onLogin(token)
    } catch {
      setError('Không kết nối được server.')
    } finally {
      setLoading(false)
    }
  }

  const inputStyle = (field) => ({
    width: '100%',
    padding: '13px 14px 13px 42px',
    fontSize: 14,
    border: `1.5px solid ${focused === field ? '#1a73e8' : 'var(--c-border)'}`,
    borderRadius: 10,
    background: 'var(--c-app)',
    color: 'var(--c-text)',
    outline: 'none',
    fontFamily: 'inherit',
    transition: 'border-color 0.2s, box-shadow 0.2s',
    boxShadow: focused === field ? '0 0 0 3px rgba(26,115,232,0.12)' : 'none',
  })

  return (
    <>
      <style>{`
        @keyframes fadeIn {
          from { opacity: 0; transform: translateX(20px); }
          to   { opacity: 1; transform: translateX(0); }
        }
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
        .form-panel {
          animation: fadeIn 0.4s cubic-bezier(0.16,1,0.3,1) both;
        }
        .login-btn {
          transition: all 0.2s cubic-bezier(0.16,1,0.3,1) !important;
        }
        .login-btn:hover:not(:disabled) {
          background: linear-gradient(135deg,#1557b0 0%,#5b32c7 100%) !important;
          transform: translateY(-1px);
          box-shadow: 0 8px 24px rgba(26,115,232,0.4) !important;
        }
        .login-btn:active:not(:disabled) {
          transform: translateY(0) !important;
        }
        .eye-btn:hover { color: var(--c-text) !important; }
      `}</style>

      <div style={{ minHeight: '100vh', display: 'flex' }}>

        {/* ── Left: image panel ── */}
        <div style={{
          flex: '0 0 58%',
          position: 'relative',
          overflow: 'hidden',
        }}>
          <img
            src="/image/social-media.webp"
            alt="Social Media"
            style={{
              width: '100%', height: '100%',
              objectFit: 'cover', objectPosition: 'center',
              display: 'block',
            }}
          />

          {/* Dark gradient overlay for readability */}
          <div style={{
            position: 'absolute', inset: 0,
            background: 'linear-gradient(to bottom, rgba(0,0,0,0.08) 0%, rgba(0,0,0,0.55) 100%)',
          }} />

          {/* Branding text */}
          <div style={{
            position: 'absolute', bottom: 44, left: 44, right: 44,
            color: '#fff',
          }}>
            <div style={{
              display: 'inline-flex', alignItems: 'center', gap: 10,
              background: 'rgba(255,255,255,0.15)',
              backdropFilter: 'blur(8px)',
              border: '1px solid rgba(255,255,255,0.25)',
              borderRadius: 10, padding: '6px 14px',
              marginBottom: 16,
            }}>
              <svg width="16" height="16" viewBox="0 0 40 40" fill="none">
                <rect x="4"  y="22" width="7" height="14" rx="2" fill="white" opacity="0.85"/>
                <rect x="16" y="14" width="7" height="22" rx="2" fill="white"/>
                <rect x="28" y="8"  width="7" height="28" rx="2" fill="white"/>
              </svg>
              <span style={{ fontSize: 12, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase' }}>
                Analytics Platform
              </span>
            </div>
            <h2 style={{ fontSize: 30, fontWeight: 700, margin: '0 0 10px', lineHeight: 1.2 }}>
              Turn social data<br />into actionable insights.
            </h2>
            <p style={{ fontSize: 14, opacity: 0.8, margin: 0, lineHeight: 1.6 }}>
              Monitor pipelines, track trends, and explore<br />
              your audience — all in one place.
            </p>
          </div>
        </div>

        {/* ── Right: form panel ── */}
        <div
          className="form-panel"
          style={{
            flex: 1,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            background: 'var(--c-surface)',
            padding: '48px 40px',
          }}
        >
          <div style={{ width: '100%', maxWidth: 360 }}>

            {/* Logo + title */}
            <div style={{ marginBottom: 36 }}>
              <div style={{
                width: 56, height: 56, borderRadius: 14,
                background: 'linear-gradient(135deg, #1a73e8 0%, #6c47ff 100%)',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                marginBottom: 20,
                boxShadow: '0 6px 20px rgba(26,115,232,0.30)',
              }}>
                <svg width="28" height="28" viewBox="0 0 40 40" fill="none">
                  <rect x="4"  y="22" width="7" height="14" rx="2" fill="white" opacity="0.80"/>
                  <rect x="16" y="14" width="7" height="22" rx="2" fill="white" opacity="0.92"/>
                  <rect x="28" y="8"  width="7" height="28" rx="2" fill="white"/>
                  <path d="M7.5 20 L19.5 12 L31.5 6" stroke="white" strokeWidth="2" strokeLinecap="round" opacity="0.55"/>
                  <circle cx="7.5"  cy="20" r="2" fill="white" opacity="0.55"/>
                  <circle cx="19.5" cy="12" r="2" fill="white" opacity="0.55"/>
                  <circle cx="31.5" cy="6"  r="2" fill="white" opacity="0.55"/>
                </svg>
              </div>
              <h1 style={{ fontSize: 22, fontWeight: 700, color: 'var(--c-text)', margin: '0 0 6px' }}>
                Welcome back
              </h1>
              <p style={{ fontSize: 14, color: 'var(--c-muted)', margin: 0 }}>
                Sign in to Social Media Analytics
              </p>
            </div>

            {/* Form */}
            <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

              {/* Username */}
              <div>
                <label style={{
                  fontSize: 11.5, fontWeight: 600, color: 'var(--c-muted)',
                  letterSpacing: '0.06em', textTransform: 'uppercase',
                  display: 'block', marginBottom: 8,
                }}>
                  Username
                </label>
                <div style={{ position: 'relative' }}>
                  <span style={{
                    position: 'absolute', left: 14, top: '50%', transform: 'translateY(-50%)',
                    color: focused === 'username' ? '#1a73e8' : 'var(--c-muted)',
                    transition: 'color 0.2s', pointerEvents: 'none', display: 'flex',
                  }}>
                    <UserIcon />
                  </span>
                  <input
                    type="text"
                    value={username}
                    onChange={e => setUsername(e.target.value)}
                    onFocus={() => setFocused('username')}
                    onBlur={() => setFocused(null)}
                    required
                    autoFocus
                    placeholder="Enter your username"
                    style={inputStyle('username')}
                  />
                </div>
              </div>

              {/* Password */}
              <div>
                <label style={{
                  fontSize: 11.5, fontWeight: 600, color: 'var(--c-muted)',
                  letterSpacing: '0.06em', textTransform: 'uppercase',
                  display: 'block', marginBottom: 8,
                }}>
                  Password
                </label>
                <div style={{ position: 'relative' }}>
                  <span style={{
                    position: 'absolute', left: 14, top: '50%', transform: 'translateY(-50%)',
                    color: focused === 'password' ? '#1a73e8' : 'var(--c-muted)',
                    transition: 'color 0.2s', pointerEvents: 'none', display: 'flex',
                  }}>
                    <LockIcon />
                  </span>
                  <input
                    type={showPass ? 'text' : 'password'}
                    value={password}
                    onChange={e => setPassword(e.target.value)}
                    onFocus={() => setFocused('password')}
                    onBlur={() => setFocused(null)}
                    required
                    placeholder="Enter your password"
                    style={{ ...inputStyle('password'), paddingRight: 44 }}
                  />
                  <button
                    type="button"
                    className="eye-btn"
                    onClick={() => setShowPass(v => !v)}
                    style={{
                      position: 'absolute', right: 12, top: '50%', transform: 'translateY(-50%)',
                      background: 'none', border: 'none', cursor: 'pointer',
                      color: 'var(--c-muted)', padding: 4, borderRadius: 4,
                      display: 'flex', alignItems: 'center', transition: 'color 0.15s',
                    }}
                  >
                    <EyeIcon open={showPass} />
                  </button>
                </div>
              </div>

              {/* Error */}
              {error && (
                <div style={{
                  padding: '10px 14px',
                  background: 'rgba(234,67,53,0.07)',
                  border: '1px solid rgba(234,67,53,0.22)',
                  borderRadius: 10,
                  fontSize: 13, color: '#c5221f',
                  display: 'flex', alignItems: 'center', gap: 8,
                }}>
                  <AlertIcon />
                  {error}
                </div>
              )}

              {/* Submit */}
              <button
                type="submit"
                disabled={loading}
                className="login-btn"
                style={{
                  width: '100%',
                  padding: '13px 0',
                  background: loading
                    ? 'linear-gradient(135deg, #93b8f5 0%, #a78bfa 100%)'
                    : 'linear-gradient(135deg, #1a73e8 0%, #6c47ff 100%)',
                  color: '#fff',
                  border: 'none',
                  borderRadius: 10,
                  fontSize: 14, fontWeight: 600,
                  cursor: loading ? 'not-allowed' : 'pointer',
                  fontFamily: 'inherit',
                  marginTop: 6,
                  letterSpacing: '0.02em',
                  boxShadow: loading ? 'none' : '0 4px 16px rgba(26,115,232,0.28)',
                  display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8,
                }}
              >
                {loading ? (
                  <>
                    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"
                      style={{ animation: 'spin 0.75s linear infinite' }}>
                      <path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83"/>
                    </svg>
                    Signing in…
                  </>
                ) : 'Sign In →'}
              </button>
            </form>
          </div>
        </div>

      </div>
    </>
  )
}
