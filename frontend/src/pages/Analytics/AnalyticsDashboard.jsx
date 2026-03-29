import React, { useState, useEffect } from 'react';

const API_KEY = "gmp79b9qSN}&JWX";

const AnalyticsDashboard = () => {
  const [dashboards, setDashboards] = useState([]);
  const [activeDashId, setActiveDashId] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchDashboards = async () => {
      try {
        const res = await fetch('/api/v1/metabase/dashboards', {
          headers: { 'API-Key': API_KEY }
        });
        if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
        const data = await res.json();
        setDashboards(data);
        if (data.length > 0) setActiveDashId(data[0].id);
      } catch (err) {
        console.error("Failed to fetch Metabase dashboards:", err);
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };
    fetchDashboards();
  }, []);

  if (loading) return <div className="loading-container">Syncing BI Core...</div>;
  if (error) return <div className="error-container">BI Connection Error: {error}</div>;
  if (dashboards.length === 0) return <div className="empty-container">No active BI dashboards found.</div>;

  const currentDash = dashboards.find(d => d.id === activeDashId) || dashboards[0];

  return (
    <div className="analytics-container">
      {dashboards.length > 1 && (
        <div className="dash-tabs">
          {dashboards.map(d => (
            <button 
              key={d.id} 
              className={`dash-tab ${activeDashId === d.id ? 'active' : ''}`}
              onClick={() => setActiveDashId(d.id)}
            >
              {d.name}
            </button>
          ))}
        </div>
      )}
      <div className="iframe-wrapper">
        <iframe
          src={currentDash.embed_url}
          frameBorder="0"
          width="100%"
          height="100%"
          allowTransparency
          title={currentDash.name}
        ></iframe>
      </div>
    </div>
  );
};

export default AnalyticsDashboard;
