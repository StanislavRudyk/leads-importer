import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';

const API_KEY = "gmp79b9qSN}&JWX";

const API_BASE = '';

const AnalyticsDashboard = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const [dashboards, setDashboards] = useState([]);
  const [activeDashId, setActiveDashId] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchDashboards = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/v1/metabase/dashboards`, {
          headers: { 'API-Key': API_KEY }
        });
        if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
        const result = await res.json();
        const data = result.dashboards || [];
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

  const currentDash = dashboards.find(d => d.id === activeDashId) || dashboards[0];

  const renderContent = () => {
    if (loading) return <div className="loading-container">Syncing BI Core...</div>;
    if (error) return <div className="error-container">BI Connection Error: {error}</div>;
    if (dashboards.length === 0) return <div className="empty-container">No active BI dashboards found.</div>;

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
                {d.title || d.name}
              </button>
            ))}
          </div>
        )}
        <div className="iframe-wrapper">
          <iframe
            src={currentDash?.url || currentDash?.embed_url}
            frameBorder="0"
            width="100%"
            height="100%"
            allowTransparency
            title={currentDash?.title || currentDash?.name}
          ></iframe>
        </div>
      </div>
    );
  };

  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      <div className="topbar">
        <div className="logo"><b style={{color: '#fff'}}>LEADS IMPORTER</b> <span style={{color: '#6b7280'}}>• BI Analytics</span></div>
        <div className="hacts"></div>
      </div>
      <div className="page" style={{ display: 'flex', flex: 1 }}>
        <div className="sidebar">
          <div>
            <div className="sl">Main Menu</div>
            <div style={{display:'flex', flexDirection:'column', gap:'5px', marginTop:'10px'}}>
              <button 
                className="fbtn"
                onClick={() => navigate('/')}
                style={{justifyContent: 'flex-start', borderLeft: '3px solid transparent', textDecoration: 'none'}}
              >
                <span className="rdot" style={{marginRight:'8px'}}></span>
                Overview
              </button>
              <button 
                className="fbtn active"
                style={{justifyContent: 'flex-start', borderLeft: '3px solid #3b82f6', textDecoration: 'none'}}
              >
                <span className="rdot" style={{marginRight:'8px'}}></span>
                BI Analytics
              </button>
            </div>
          </div>
          <div className="dvd"></div>
          <div className="sidebar-footer">
            <div className="sl">System Link</div>
            <a href="/nocodb/" target="_blank" rel="noreferrer" className="fbtn" style={{textDecoration:'none'}}>
              <span className="rdot" style={{marginRight:'8px', background:'#10b981'}}></span>
              NocoDB Tables
            </a>
          </div>
        </div>
        <div className="content">
          {renderContent()}
        </div>
      </div>
    </div>
  );
};

export default AnalyticsDashboard;
