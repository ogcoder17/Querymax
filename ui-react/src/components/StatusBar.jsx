export default function StatusBar({ text, type }) {
  return <div className={`status ${type || ''}`}>{text || ''}</div>;
}
