export default function DataTable({ columns = [], rows = [] }) {
  if (!columns.length) {
    return (
      <div className="table">
        <table>
          <thead><tr><th>No columns</th></tr></thead>
        </table>
      </div>
    );
  }
  return (
    <div className="table">
      <table>
        <thead>
          <tr>{columns.map((c, i) => <th key={i}>{c}</th>)}</tr>
        </thead>
        <tbody>
          {rows.map((r, i) =>
            <tr key={i}>{r.map((v, j) => <td key={j}>{v !== null && v !== undefined ? String(v) : ''}</td>)}</tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
