import React, { useState, useRef } from 'react';

const API = '';  // same origin

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
          <h1 style={styles.title}>FA Rule Converter</h1>
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
        <h1 style={styles.title}>FA Rule Converter</h1>

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
    background: '#fff', borderRadius: 12, padding: '48px 40px',
    boxShadow: '0 4px 24px rgba(0,0,0,0.08)', maxWidth: 380, width: '100%',
    textAlign: 'center', marginTop: '10vh',
  },
  card: {
    background: '#fff', borderRadius: 12, padding: '36px 40px',
    boxShadow: '0 4px 24px rgba(0,0,0,0.08)', maxWidth: 600, width: '100%',
  },
  title: {
    margin: '0 0 8px', color: '#2E5A88', fontSize: 28, fontWeight: 700,
  },
  subtitle: {
    color: '#666', margin: '0 0 24px', fontSize: 14,
  },
  section: {
    marginBottom: 18,
  },
  label: {
    display: 'block', fontWeight: 600, fontSize: 13, color: '#333',
    marginBottom: 6, textTransform: 'uppercase', letterSpacing: 0.5,
  },
  radioGroup: {
    display: 'flex', gap: 16, flexWrap: 'wrap',
  },
  radioLabel: {
    fontSize: 14, color: '#444', cursor: 'pointer', display: 'flex',
    alignItems: 'center', gap: 4,
  },
  input: {
    width: '100%', padding: '10px 12px', border: '1px solid #ddd',
    borderRadius: 6, fontSize: 14, boxSizing: 'border-box',
    outline: 'none',
  },
  fileInput: {
    fontSize: 13, color: '#444',
  },
  primaryBtn: {
    background: '#2E5A88', color: '#fff', border: 'none', borderRadius: 8,
    padding: '12px 32px', fontSize: 15, fontWeight: 600, cursor: 'pointer',
    width: '100%', marginTop: 8,
  },
  progressSection: {
    marginTop: 20, padding: '16px', background: '#f0f4f8', borderRadius: 8,
  },
  stageText: {
    fontSize: 13, fontWeight: 600, color: '#2E5A88', marginBottom: 8,
  },
  progressTrack: {
    height: 8, background: '#dde3ea', borderRadius: 4, overflow: 'hidden',
  },
  progressFill: {
    height: '100%', background: '#2E5A88', borderRadius: 4,
    transition: 'width 0.3s ease',
  },
  pctText: {
    fontSize: 12, color: '#888', marginTop: 4, textAlign: 'right',
  },
  error: {
    color: '#c00', marginTop: 12, fontSize: 13,
  },
  errorBox: {
    marginTop: 16, padding: 14, background: '#fef2f2', border: '1px solid #fca5a5',
    borderRadius: 8, color: '#991b1b', fontSize: 13, whiteSpace: 'pre-wrap',
  },
  successBox: {
    marginTop: 16, padding: 16, background: '#f0fdf4', border: '1px solid #86efac',
    borderRadius: 8,
  },
  summaryPre: {
    margin: '0 0 12px', fontSize: 13, color: '#166534', whiteSpace: 'pre-wrap',
    fontFamily: 'inherit', lineHeight: 1.5,
  },
  downloadBtn: {
    display: 'inline-block', background: '#16a34a', color: '#fff',
    padding: '10px 24px', borderRadius: 6, textDecoration: 'none',
    fontWeight: 600, fontSize: 14,
  },
};

export default App;
