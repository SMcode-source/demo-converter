import React, { useState, useRef } from 'react';

const API = '';  // same origin

// ── Futuristic House Logo SVG ──
function AiLogo({ size = 80 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 120 120" fill="none" xmlns="http://www.w3.org/2000/svg">
      {/* Background glow */}
      <circle cx="60" cy="60" r="55" fill="url(#bgGlow)" opacity="0.08" />
      {/* House body */}
      <rect x="30" y="58" width="60" height="36" rx="3" fill="url(#houseBody)" />
      {/* Roof */}
      <path d="M20 62 L60 30 L100 62" stroke="url(#roofGrad)" strokeWidth="4" strokeLinecap="round" strokeLinejoin="round" fill="none" />
      <path d="M26 60 L60 34 L94 60" fill="url(#roofFill)" opacity="0.5" />
      {/* Door with AI glow */}
      <rect x="50" y="72" width="20" height="22" rx="2" fill="#1a1a2e" />
      <rect x="52" y="74" width="16" height="18" rx="1" fill="url(#doorGlow)" opacity="0.8" />
      {/* AI eye/sensor above door */}
      <circle cx="60" cy="66" r="4" fill="#0ff" opacity="0.9" />
      <circle cx="60" cy="66" r="6" stroke="#0ff" strokeWidth="1" opacity="0.4" />
      <circle cx="60" cy="66" r="9" stroke="#0ff" strokeWidth="0.5" opacity="0.2" />
      {/* Windows with circuit glow */}
      <rect x="34" y="64" width="12" height="10" rx="1.5" fill="#1a1a2e" />
      <rect x="35" y="65" width="10" height="8" rx="1" fill="url(#windowGlow)" opacity="0.7" />
      <rect x="74" y="64" width="12" height="10" rx="1.5" fill="#1a1a2e" />
      <rect x="75" y="65" width="10" height="8" rx="1" fill="url(#windowGlow)" opacity="0.7" />
      {/* Circuit lines on house */}
      <line x1="40" y1="80" x2="40" y2="90" stroke="#0ff" strokeWidth="0.8" opacity="0.3" />
      <line x1="80" y1="80" x2="80" y2="90" stroke="#0ff" strokeWidth="0.8" opacity="0.3" />
      <line x1="34" y1="85" x2="46" y2="85" stroke="#0ff" strokeWidth="0.8" opacity="0.3" />
      <line x1="74" y1="85" x2="86" y2="85" stroke="#0ff" strokeWidth="0.8" opacity="0.3" />
      {/* Circuit nodes */}
      <circle cx="40" cy="80" r="1.5" fill="#0ff" opacity="0.5" />
      <circle cx="80" cy="80" r="1.5" fill="#0ff" opacity="0.5" />
      <circle cx="40" cy="90" r="1.5" fill="#0ff" opacity="0.5" />
      <circle cx="80" cy="90" r="1.5" fill="#0ff" opacity="0.5" />
      {/* Antenna / signal */}
      <line x1="60" y1="30" x2="60" y2="18" stroke="#6B9FD4" strokeWidth="2" strokeLinecap="round" />
      <circle cx="60" cy="15" r="3" fill="#0ff" opacity="0.8" />
      <circle cx="60" cy="15" r="6" stroke="#0ff" strokeWidth="0.8" opacity="0.3" />
      <circle cx="60" cy="15" r="10" stroke="#0ff" strokeWidth="0.5" opacity="0.15" />
      {/* Ground line */}
      <line x1="18" y1="94" x2="102" y2="94" stroke="#4A90D9" strokeWidth="1.5" opacity="0.3" strokeLinecap="round" />
      <defs>
        <linearGradient id="bgGlow" x1="0" y1="0" x2="120" y2="120">
          <stop offset="0%" stopColor="#4A90D9" />
          <stop offset="100%" stopColor="#7C3AED" />
        </linearGradient>
        <linearGradient id="houseBody" x1="30" y1="58" x2="90" y2="94">
          <stop offset="0%" stopColor="#1e293b" />
          <stop offset="100%" stopColor="#0f172a" />
        </linearGradient>
        <linearGradient id="roofGrad" x1="20" y1="62" x2="100" y2="30">
          <stop offset="0%" stopColor="#4A90D9" />
          <stop offset="100%" stopColor="#7C3AED" />
        </linearGradient>
        <linearGradient id="roofFill" x1="26" y1="60" x2="60" y2="34">
          <stop offset="0%" stopColor="#4A90D9" />
          <stop offset="100%" stopColor="#6366F1" />
        </linearGradient>
        <linearGradient id="doorGlow" x1="52" y1="74" x2="68" y2="92">
          <stop offset="0%" stopColor="#0ff" stopOpacity="0.4" />
          <stop offset="100%" stopColor="#7C3AED" stopOpacity="0.2" />
        </linearGradient>
        <linearGradient id="windowGlow" x1="0" y1="0" x2="10" y2="8">
          <stop offset="0%" stopColor="#0ff" stopOpacity="0.6" />
          <stop offset="100%" stopColor="#4A90D9" stopOpacity="0.3" />
        </linearGradient>
      </defs>
    </svg>
  );
}

function App() {
  const [authed, setAuthed] = useState(false);
  const [password, setPassword] = useState('');
  const [loginError, setLoginError] = useState('');

  const [mode, setMode] = useState('LTA');
  const [action, setAction] = useState('convert');
  const [threshold, setThreshold] = useState('0.01');

  const [keyFile, setKeyFile] = useState(null);
  const [faFile, setFaFile] = useState(null);
  const [matFile, setMatFile] = useState(null);

  const [running, setRunning] = useState(false);
  const [stage, setStage] = useState('');
  const [progress, setProgress] = useState(0);
  const [maxProgress, setMaxProgress] = useState(1);
  const [summary, setSummary] = useState('');
  const [error, setError] = useState('');
  const [downloadUrl, setDownloadUrl] = useState('');

  const abortRef = useRef(null);

  // ── Login ──
  const handleLogin = async (e) => {
    e.preventDefault();
    setLoginError('');
    const form = new FormData();
    form.append('password', password);
    try {
      const res = await fetch(`${API}/api/login`, { method: 'POST', body: form, credentials: 'include' });
      if (res.ok) {
        setAuthed(true);
      } else {
        setLoginError('Wrong password');
      }
    } catch {
      setLoginError('Connection failed');
    }
  };

  // ── Run conversion ──
  const handleRun = async () => {
    if (!keyFile || !faFile) { setError('Please select Key file and FA file.'); return; }
    if (mode === 'FTA' && !matFile) { setError('FTA mode requires the FA MAT file.'); return; }

    setRunning(true);
    setSummary('');
    setError('');
    setDownloadUrl('');
    setStage('Uploading files...');
    setProgress(0);
    setMaxProgress(1);

    const form = new FormData();
    form.append('mode', mode);
    form.append('action', action);
    form.append('threshold', threshold);
    form.append('key_file', keyFile);
    form.append('fa_file', faFile);
    if (matFile) form.append('fa_mat_file', matFile);

    try {
      const res = await fetch(`${API}/api/convert`, {
        method: 'POST', body: form, credentials: 'include'
      });
      if (!res.ok) {
        const err = await res.json();
        setError(err.detail || 'Upload failed');
        setRunning(false);
        return;
      }
      const { job_id } = await res.json();
      setDownloadUrl(`${API}/api/download/${job_id}`);

      // Listen for SSE progress
      const evtSource = new EventSource(`${API}/api/progress/${job_id}`);
      abortRef.current = evtSource;

      evtSource.onmessage = (event) => {
        const data = JSON.parse(event.data);

        if (data.status === 'done') {
          setStage('Complete');
          setProgress(1);
          setMaxProgress(1);
          setSummary(data.summary || 'Done');
          setRunning(false);
          evtSource.close();
          return;
        }
        if (data.status === 'error') {
          setError(data.error || 'Processing failed');
          setRunning(false);
          evtSource.close();
          return;
        }
        if (data.stage) {
          setStage(data.stage);
          setProgress(data.step);
          setMaxProgress(data.total);
        }
      };

      evtSource.onerror = () => {
        evtSource.close();
        if (!summary && !error) setError('Lost connection to server');
        setRunning(false);
      };

    } catch (e) {
      setError(`Request failed: ${e.message}`);
      setRunning(false);
    }
  };

  const pct = maxProgress > 0 ? Math.round((progress / maxProgress) * 100) : 0;

  // ── Login screen ──
  if (!authed) {
    return (
      <div style={styles.center}>
        <div style={styles.loginCard}>
          <AiLogo size={90} />
          <h1 style={styles.brandTitle}>Sappy's Home of AI</h1>
          <h2 style={styles.toolTitle}>FA Rule Converter</h2>
          <p style={styles.subtitle}>Enter password to continue</p>
          <form onSubmit={handleLogin}>
            <input
              type="password"
              value={password}
              onChange={e => setPassword(e.target.value)}
              placeholder="Password"
              style={styles.input}
              autoFocus
            />
            <button type="submit" style={styles.primaryBtn}>Log In</button>
          </form>
          {loginError && <p style={styles.error}>{loginError}</p>}
        </div>
      </div>
    );
  }

  // ── Main app ──
  return (
    <div style={styles.center}>
      <div style={styles.card}>
        <div style={{textAlign: 'center', marginBottom: 8}}>
          <AiLogo size={60} />
        </div>
        <h1 style={styles.brandTitle}>Sappy's Home of AI</h1>
        <h2 style={styles.toolTitle}>FA Rule Converter</h2>

        {/* Mode selector */}
        <div style={styles.section}>
          <label style={styles.label}>Mode</label>
          <div style={styles.radioGroup}>
            <label style={styles.radioLabel}>
              <input type="radio" name="mode" value="LTA" checked={mode === 'LTA'}
                     onChange={() => setMode('LTA')} disabled={running} />
              LTA (single FA rule file)
            </label>
            <label style={styles.radioLabel}>
              <input type="radio" name="mode" value="FTA" checked={mode === 'FTA'}
                     onChange={() => setMode('FTA')} disabled={running} />
              FTA (INC + MAT files)
            </label>
          </div>
        </div>

        {/* File inputs */}
        <div style={styles.section}>
          <label style={styles.label}>Key File (.xlsx)</label>
          <input type="file" accept=".xlsx,.xls" onChange={e => setKeyFile(e.target.files[0])}
                 disabled={running} style={styles.fileInput} />
        </div>

        <div style={styles.section}>
          <label style={styles.label}>{mode === 'FTA' ? 'FA INC File (.xlsx)' : 'FA Rule File (.xlsx)'}</label>
          <input type="file" accept=".xlsx,.xls" onChange={e => setFaFile(e.target.files[0])}
                 disabled={running} style={styles.fileInput} />
        </div>

        {mode === 'FTA' && (
          <div style={styles.section}>
            <label style={styles.label}>FA MAT File (.xlsx)</label>
            <input type="file" accept=".xlsx,.xls" onChange={e => setMatFile(e.target.files[0])}
                   disabled={running} style={styles.fileInput} />
          </div>
        )}

        {/* Action selector */}
        <div style={styles.section}>
          <label style={styles.label}>Action</label>
          <div style={styles.radioGroup}>
            <label style={styles.radioLabel}>
              <input type="radio" name="action" value="convert" checked={action === 'convert'}
                     onChange={() => setAction('convert')} disabled={running} />
              Convert
            </label>
            <label style={styles.radioLabel}>
              <input type="radio" name="action" value="audit" checked={action === 'audit'}
                     onChange={() => setAction('audit')} disabled={running} />
              Convert + Audit
            </label>
            <label style={styles.radioLabel}>
              <input type="radio" name="action" value="group" checked={action === 'group'}
                     onChange={() => setAction('group')} disabled={running} />
              Convert + Audit + Group
            </label>
          </div>
        </div>

        {/* Threshold */}
        <div style={styles.section}>
          <label style={styles.label}>Grouping Threshold</label>
          <input type="text" value={threshold} onChange={e => setThreshold(e.target.value)}
                 disabled={running} style={{...styles.input, width: '100px'}} />
        </div>

        {/* Run button */}
        <button onClick={handleRun} disabled={running} style={{
          ...styles.primaryBtn,
          opacity: running ? 0.6 : 1,
          cursor: running ? 'not-allowed' : 'pointer',
        }}>
          {running ? 'Processing...' : 'Run'}
        </button>

        {/* Progress bar */}
        {running && (
          <div style={styles.progressSection}>
            <div style={styles.stageText}>{stage}</div>
            <div style={styles.progressTrack}>
              <div style={{...styles.progressFill, width: `${pct}%`}} />
            </div>
            <div style={styles.pctText}>{pct}%</div>
          </div>
        )}

        {/* Error */}
        {error && <div style={styles.errorBox}>{error}</div>}

        {/* Summary + Download */}
        {summary && (
          <div style={styles.successBox}>
            <pre style={styles.summaryPre}>{summary}</pre>
            {downloadUrl && (
              <a href={downloadUrl} style={styles.downloadBtn}>
                Download Results (.zip)
              </a>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ── Styles ──
const styles = {
  center: {
    display: 'flex', justifyContent: 'center', alignItems: 'flex-start',
    minHeight: '100vh', padding: '40px 16px', boxSizing: 'border-box',
  },
  loginCard: {
    background: 'rgba(30, 41, 59, 0.95)', borderRadius: 16, padding: '48px 40px',
    boxShadow: '0 8px 32px rgba(0,0,0,0.3), 0 0 60px rgba(74,144,217,0.08)',
    border: '1px solid rgba(74,144,217,0.15)',
    maxWidth: 380, width: '100%', textAlign: 'center', marginTop: '10vh',
  },
  card: {
    background: 'rgba(30, 41, 59, 0.95)', borderRadius: 16, padding: '36px 40px',
    boxShadow: '0 8px 32px rgba(0,0,0,0.3), 0 0 60px rgba(74,144,217,0.08)',
    border: '1px solid rgba(74,144,217,0.15)',
    maxWidth: 600, width: '100%',
  },
  brandTitle: {
    margin: '8px 0 2px', color: '#4A90D9', fontSize: 26, fontWeight: 700,
    textAlign: 'center', letterSpacing: 0.5,
    background: 'linear-gradient(135deg, #4A90D9, #7C3AED)',
    WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent',
  },
  toolTitle: {
    margin: '0 0 16px', color: '#64748b', fontSize: 16, fontWeight: 500,
    textAlign: 'center',
  },
  subtitle: {
    color: '#666', margin: '0 0 24px', fontSize: 14,
  },
  section: {
    marginBottom: 18,
  },
  label: {
    display: 'block', fontWeight: 600, fontSize: 13, color: '#94a3b8',
    marginBottom: 6, textTransform: 'uppercase', letterSpacing: 0.5,
  },
  radioGroup: {
    display: 'flex', gap: 16, flexWrap: 'wrap',
  },
  radioLabel: {
    fontSize: 14, color: '#cbd5e1', cursor: 'pointer', display: 'flex',
    alignItems: 'center', gap: 4,
  },
  input: {
    width: '100%', padding: '10px 12px', border: '1px solid rgba(74,144,217,0.3)',
    borderRadius: 6, fontSize: 14, boxSizing: 'border-box',
    outline: 'none', background: '#0f172a', color: '#e2e8f0',
  },
  fileInput: {
    fontSize: 13, color: '#94a3b8',
  },
  primaryBtn: {
    background: 'linear-gradient(135deg, #4A90D9, #7C3AED)', color: '#fff',
    border: 'none', borderRadius: 8,
    padding: '12px 32px', fontSize: 15, fontWeight: 600, cursor: 'pointer',
    width: '100%', marginTop: 8,
    boxShadow: '0 4px 15px rgba(74,144,217,0.3)',
  },
  progressSection: {
    marginTop: 20, padding: '16px', background: 'rgba(15,23,42,0.6)',
    borderRadius: 8, border: '1px solid rgba(0,255,255,0.1)',
  },
  stageText: {
    fontSize: 13, fontWeight: 600, color: '#0ff', marginBottom: 8,
  },
  progressTrack: {
    height: 8, background: 'rgba(74,144,217,0.15)', borderRadius: 4, overflow: 'hidden',
  },
  progressFill: {
    height: '100%', background: 'linear-gradient(90deg, #4A90D9, #0ff)',
    borderRadius: 4, transition: 'width 0.3s ease',
    boxShadow: '0 0 10px rgba(0,255,255,0.3)',
  },
  pctText: {
    fontSize: 12, color: '#64748b', marginTop: 4, textAlign: 'right',
  },
  error: {
    color: '#f87171', marginTop: 12, fontSize: 13,
  },
  errorBox: {
    marginTop: 16, padding: 14, background: 'rgba(127,29,29,0.2)',
    border: '1px solid rgba(248,113,113,0.3)',
    borderRadius: 8, color: '#fca5a5', fontSize: 13, whiteSpace: 'pre-wrap',
  },
  successBox: {
    marginTop: 16, padding: 16, background: 'rgba(5,46,22,0.3)',
    border: '1px solid rgba(74,222,128,0.3)',
    borderRadius: 8,
  },
  summaryPre: {
    margin: '0 0 12px', fontSize: 13, color: '#86efac', whiteSpace: 'pre-wrap',
    fontFamily: 'inherit', lineHeight: 1.5,
  },
  downloadBtn: {
    display: 'inline-block',
    background: 'linear-gradient(135deg, #059669, #0d9488)', color: '#fff',
    padding: '10px 24px', borderRadius: 6, textDecoration: 'none',
    fontWeight: 600, fontSize: 14,
    boxShadow: '0 4px 15px rgba(5,150,105,0.3)',
  },
};

export default App;
