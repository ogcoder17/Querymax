import { useState } from 'react';
import { api } from '../api/client';
import DataTable from '../components/DataTable';
import StatusBar from '../components/StatusBar';

const EXAMPLES = [
  'SELECT * FROM users;',
  'SELECT name, price FROM products ORDER BY price DESC LIMIT 5;',
  'SELECT o.order_id, u.name, o.total_amount FROM orders o JOIN users u ON u.id=o.user_id;',
];

export default function SqlRunnerPage() {
  const [sql, setSql] = useState('SELECT * FROM users;');
  const [allowMutations, setAllowMutations] = useState(false);
  const [status, setStatus] = useState({ text: '', type: '' });
  const [result, setResult] = useState({ columns: [], rows: [] });

  const run = async () => {
    setStatus({ text: 'Running…', type: '' });
    setResult({ columns: [], rows: [] });

    let s = (sql || '').trim();
    if (!s) return setStatus({ text: 'Please enter SQL', type: 'err' });

    if (!allowMutations) {
      const safe = /^(select|with)\s/i.test(s);
      if (!safe) {
        setStatus({ text: 'Only SELECT/WITH allowed (enable "Allow Mutations" to override)', type: 'err' });
        return;
      }
    }
    try {
      const resp = await api.runSQL(s, allowMutations);
      setResult(resp.result || { columns: [], rows: [] });
      setStatus({ text: 'Done', type: 'ok' });
    } catch (e) {
      setStatus({ text: e.message, type: 'err' });
    }
  };

  return (
    <div className="container">
      <div className="card">
        <h2>Direct SQL Runner</h2>
        <textarea rows={8} value={sql} onChange={e => setSql(e.target.value)} />
        <div className="row" style={{marginTop: 8}}>
          <button className="btn" onClick={run}>Run SQL</button>
          <label className="checkbox">
            <input type="checkbox" checked={allowMutations} onChange={e => setAllowMutations(e.target.checked)} />
            Allow INSERT/UPDATE/DELETE (danger)
          </label>
        </div>
        <StatusBar text={status.text} type={status.type} />

        <div className="result">
          <h3>Result</h3>
          <DataTable columns={result.columns || []} rows={result.rows || []} />
        </div>
      </div>

      <div className="card">
        <h4>Examples</h4>
        <div className="samples">
          {EXAMPLES.map((s, i) =>
            <button className="btn" key={i} onClick={() => setSql(s)}>{s}</button>
          )}
        </div>
      </div>
    </div>
  );
}
