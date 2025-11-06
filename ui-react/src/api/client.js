const defaultBase = import.meta.env.VITE_API_BASE || 'http://127.0.0.1:8000';

export function getApiBase() {
  return localStorage.getItem('QM_API_BASE') || defaultBase;
}
export function setApiBase(v) {
  localStorage.setItem('QM_API_BASE', v);
}

async function request(path, options = {}) {
  const base = getApiBase().replace(/\/$/, '');
  const res = await fetch(`${base}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  const text = await res.text();
  let json = {};
  try { json = text ? JSON.parse(text) : {}; } catch { /* ignore */ }
  if (!res.ok) {
    const detail = json?.detail || text || res.statusText;
    throw new Error(detail);
  }
  return json;
}

export const api = {
  ping: () => request('/'),
  schema: () => request('/schema'),
  seedDemo: () => request('/seed/demo', { method: 'POST', body: '{}' }),
  ragRebuild: () => request('/rag/rebuild', { method: 'POST', body: '{}' }),
  queryNL: (question) => request('/query', { method: 'POST', body: JSON.stringify({ question }) }),
  runSQL: (sql, allowMutations = false) =>
    request('/sql/run', { method: 'POST', body: JSON.stringify({ sql, allow_mutations: allowMutations }) }),
};
