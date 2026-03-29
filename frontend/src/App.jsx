import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import LeadsDashboard from './pages/Dashboard/LeadsDashboard';
import AnalyticsDashboard from './pages/Analytics/AnalyticsDashboard';
import Sidebar from './components/Sidebar/Sidebar';
import './analytics.css';

const App = () => {
  return (
    <Router>
      <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
        <div className="page" style={{ display: 'flex', flex: 1 }}>
          <Sidebar />
          <div className="content">
            <Routes>
              <Route path="/" element={<LeadsDashboard />} />
              <Route path="/analytics" element={<AnalyticsDashboard />} />
              <Route path="*" element={<Navigate to="/" />} />
            </Routes>
          </div>
        </div>
      </div>
    </Router>
  );
};

export default App;
