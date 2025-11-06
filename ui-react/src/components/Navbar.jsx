import { NavLink } from 'react-router-dom';
import { getApiBase, setApiBase, api } from '../api/client';
import { useState } from 'react';

export default function Navbar() {
  const [base, setBase] = useState(getApiBase());

  const saveBase = () => {
    setApiBase(base.trim());
    alert('API base saved: ' + base.trim());
  };

  const ping = async () => {
    try {
      const data = await api.ping();
      alert('Server OK: ' + JSON.stringify(data));
    } catch (e) {
      alert('Ping failed: ' + e.message);
    }
  };

  return (
    <header>
      <h1>QueryMax</h1>
      <div className="row">
        <nav className="nav">
          <NavLink to="/" className={({isActive}) => isActive ? 'active' : ''}>NL → SQL</NavLink>
          <NavLink to="/sql" className={({isActive}) => isActive ? 'active' : ''}>SQL Runner</NavLink>
        </nav>
        <input value={base} onChange={e => setBase(e.target.value)} style={{width: 280}} />
        <button className="btn" onClick={saveBase}>Save</button>
        <button className="btn" onClick={ping}>Ping</button>
      </div>
    </header>
  );
}
