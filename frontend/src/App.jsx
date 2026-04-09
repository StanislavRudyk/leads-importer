import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import LeadsDashboard from './pages/Dashboard/LeadsDashboard';
import AnalyticsDashboard from './pages/Analytics/AnalyticsDashboard';
import './analytics.css';

const App = () => {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<LeadsDashboard />} />
        <Route path="/analytics" element={<AnalyticsDashboard />} />
        <Route path="*" element={<Navigate to="/" />} />
      </Routes>
    </Router>
  );
};

export default App;
