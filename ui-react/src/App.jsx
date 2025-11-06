import { Routes, Route } from 'react-router-dom';
import NLSqlPage from './pages/NLSqlPage';
import SqlRunnerPage from './pages/SqlRunnerPage';
import Navbar from './components/Navbar';

export default function App() {
  return (
    <>
      <Navbar />
      <Routes>
        <Route path="/" element={<NLSqlPage />} />
        <Route path="/sql" element={<SqlRunnerPage />} />
      </Routes>
    </>
  );
}
