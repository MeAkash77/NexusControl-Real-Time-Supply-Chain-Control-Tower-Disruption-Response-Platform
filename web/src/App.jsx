import { useEffect, useState } from "react";
const SEV = { 4: "#b3261e", 3: "#c75b12", 2: "#8a7400", 1: "#4c6b4c" };
const API = "http://localhost:5000/api/v1";

export default function App() {
  const [token, setToken] = useState(localStorage.getItem("jwt") || "");
  const [rows, setRows] = useState([]);
  const [err, setErr] = useState("");
  const headers = { Authorization: `Bearer ${token}`, "Content-Type": "application/json" };

  const load = () => fetch(`${API}/incidents`, { headers })
    .then(r => r.ok ? r.json() : Promise.reject(r.status === 401 ? "Sign in with a valid token." : "Could not load incidents."))
    .then(d => { setRows(d); setErr(""); }).catch(setErr);

  useEffect(() => { if (token) { load(); const t = setInterval(load, 5000); return () => clearInterval(t); } }, [token]);

  const resolve = async id => {
    const note = prompt("Remediation taken:"); if (!note) return;
    await fetch(`${API}/incidents/${id}/resolve`, { method: "POST", headers, body: JSON.stringify({ remediation: note }) });
    load();
  };

  return (
    <main style={{ font: "15px system-ui", maxWidth: 960, margin: "2rem auto", padding: "0 1rem" }}>
      <h1>Open incidents</h1>
      <input placeholder="Paste access token" value={token} style={{ width: "100%" }}
        onChange={e => { setToken(e.target.value); localStorage.setItem("jwt", e.target.value); }} />
      {err && <p role="alert" style={{ color: SEV[4] }}>{err}</p>}
      {!err && token && rows.length === 0 && <p>No open incidents. New disruptions appear here within seconds.</p>}
      <table style={{ width: "100%", borderCollapse: "collapse", marginTop: 16 }}>
        <thead><tr><th align="left">Severity</th><th align="left">Type</th><th align="left">Entity</th><th>Alerts</th><th align="left">SLA</th><th /></tr></thead>
        <tbody>{rows.map(r => (
          <tr key={r.id} style={{ borderTop: "1px solid #ddd" }}>
            <td style={{ color: SEV[r.severity], fontWeight: 600 }}>S{r.severity}</td>
            <td>{r.type}</td><td>{r.entityKey}</td><td align="center">{r.alertCount}</td>
            <td style={{ color: r.slaBreached ? SEV[4] : "inherit" }}>{r.slaBreached ? "Breached" : new Date(r.slaDueAt + "Z").toLocaleTimeString()}</td>
            <td><button onClick={() => resolve(r.id)}>Resolve</button></td>
          </tr>))}</tbody>
      </table>
    </main>
  );
}
