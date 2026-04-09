import React, { useState, useEffect, useMemo } from 'react';
import {
  Chart as ChartJS, CategoryScale, LinearScale,
  BarElement, ArcElement, Title, Tooltip, Legend
} from 'chart.js';
import { Bar, Doughnut } from 'react-chartjs-2';

ChartJS.register(CategoryScale, LinearScale, BarElement, ArcElement, Title, Tooltip, Legend);

const RC = { 'USA': '#3b82f6', 'Europe': '#8b5cf6', 'Oceania': '#10b981', 'Asia': '#f59e0b', 'Middle East': '#ef4444', 'Canada': '#22d3ee', 'Other': '#6b7280' };
const API_KEY = "gmp79b9qSN}&JWX";

function fmt(n) { return n == null ? '—' : Number(n).toLocaleString('en'); }

// Определяем базовый URL для API (бэкенд на порту 8000)
const API_BASE = '';

const LeadsDashboard = () => {
  const [userRole, setUserRole] = useState('admin');
  const [activeTab, setActiveTab] = useState(15);
  const [loading, setLoading] = useState(true);
  const [importing, setImporting] = useState(false);
  const [message, setMessage] = useState(null);

  const [data, setData] = useState([]);
  const [search, setSearch] = useState('');
  const [fReg, setFReg] = useState('all');
  const [viewMode, setViewMode] = useState('city');

  const [campaignData, setCampaignData] = useState([]);
  const [importData, setImportData] = useState([]);

  const fetchData = async (isBackground = false) => {
    if (!isBackground) setLoading(true);
    try {
      if (activeTab === 15) {
        const r = await fetch(`${API_BASE}/api/v1/dashboard/overview`);
        if (!r.ok) throw new Error("API Error");
        const d = await r.json();
        setData(Array.isArray(d) ? d : []);
      } else if (activeTab === 16) {
        const r = await fetch(`${API_BASE}/api/v1/dashboard/sources`);
        if (!r.ok) throw new Error("API Error");
        const d = await r.json();
        setCampaignData(Array.isArray(d) ? d : []);
      } else if (activeTab === 17) {
        const r = await fetch(`${API_BASE}/api/v1/dashboard/imports`);
        if (!r.ok) throw new Error("API Error");
        const d = await r.json();
        setImportData(Array.isArray(d) ? d : []);
      }
    } catch (e) {
      console.error("Fetch error:", e);
    }
    if (!isBackground) setLoading(false);
  };

  useEffect(() => {
    fetchData(); // Initial load
    const interval = setInterval(() => {
      fetchData(true); // Silent re-fetch every 5 seconds
    }, 5000);
    return () => clearInterval(interval);
  }, [activeTab]);

  const availableTabs = useMemo(() => {
    if (userRole === 'admin') return [{id: 15, title: 'Leads Overview'}, {id: 16, title: 'Campaigns & Sources'}, {id: 17, title: 'System & Imports'}];
    if (userRole === 'manager') return [{id: 15, title: 'Leads Overview'}, {id: 16, title: 'Campaigns & Sources'}];
    return [{id: 15, title: 'Leads Overview'}];
  }, [userRole]);

  useEffect(() => {
    if (!availableTabs.find(t => t.id === activeTab)) setActiveTab(15);
  }, [userRole, availableTabs, activeTab]);

  const handleFileUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    setImporting(true);
    setMessage({ text: "Uploading file for background processing...", type: "info" });

    const formData = new FormData();
    formData.append("file", file);

    try {
      const res = await fetch(`${API_BASE}/api/v1/import/upload?source_name=${encodeURIComponent(file.name)}`, {
        method: "POST",
        headers: { "API-Key": API_KEY },
        body: formData
      });
      const result = await res.json();
      if (res.ok) {
        setMessage({ text: "Success! Import started in background. Refresh in a few seconds.", type: "success" });
      } else {
        setMessage({ text: `Error: ${result.detail || "Upload failed"}`, type: "error" });
      }
    } catch (err) {
      setMessage({ text: "Network error during upload.", type: "error" });
    }
    setImporting(false);
    setTimeout(() => setMessage(null), 5000);
  };

  const handleFolderUpload = async (e) => {
    const rawFiles = e.target.files;
    if (!rawFiles || rawFiles.length === 0) return;
    
    const files = Array.from(rawFiles).filter(f => !f.name.startsWith('.'));
    if (!files.length) return;

    setImporting(true);
    let successCount = 0;
    let errorCount = 0;
    let completed = 0;
    const concurrencyLimit = 5; // Parallel workers

    const processFile = async (file) => {
        const formData = new FormData();
        formData.append("file", file);
        const sourcePath = file.webkitRelativePath || file.name;

        try {
            const res = await fetch(`${API_BASE}/api/v1/import/upload?source_name=${encodeURIComponent(sourcePath)}`, {
                method: "POST",
                headers: { "API-Key": API_KEY },
                body: formData
            });
            if (res.ok) successCount++;
            else errorCount++;
        } catch (err) {
            errorCount++;
        }
        completed++;
        setMessage({ text: `Uploading... ${completed} of ${files.length} files processed`, type: "info" });
    };

    setMessage({ text: `Starting parallel upload of ${files.length} files...`, type: "info" });
    
    // Execute pool
    const queue = [...files];
    const workers = Array(Math.min(concurrencyLimit, queue.length)).fill(null).map(async () => {
        while (queue.length > 0) {
            await processFile(queue.shift());
        }
    });

    await Promise.all(workers);
    
    setMessage({ text: `Folder upload complete. ${successCount} successful, ${errorCount} failed.`, type: successCount > 0 ? "success" : "error" });
    setImporting(false);
    setTimeout(() => setMessage(null), 10000);
  };

  const filteredData = useMemo(() => {
    let res = [...data];
    if (search) res = res.filter(d => (d.city||'').toLowerCase().includes(search.toLowerCase()) || (d.country||'').toLowerCase().includes(search.toLowerCase()) || d.region.toLowerCase().includes(search.toLowerCase()));
    if (fReg !== 'all') res = res.filter(d => d.region === fReg);
    return res;
  }, [data, search, fReg]);

  const aggregatedData = useMemo(() => {
    if (viewMode === 'city') return filteredData.map(d => ({ ...d, label: `${d.city} / ${d.country}` })).sort((a,b) => b.leads - a.leads);
    const grouped = {};
    filteredData.forEach(d => {
      const cCode = d.country || "XX";
      if (!grouped[cCode]) grouped[cCode] = { id: cCode, label: cCode, country: cCode, region: d.region, leads: 0, leads_7d: 0, status: 'upcoming', cities: 0 };
      grouped[cCode].leads += (d.leads || 0);
      grouped[cCode].leads_7d += (d.leads_7d || 0);
      grouped[cCode].cities += 1;
      if (d.status === 'done') grouped[cCode].status = 'done';
    });
    return Object.values(grouped).sort((a,b) => b.leads - a.leads);
  }, [filteredData, viewMode]);

  const tab1Stats = useMemo(() => {
    const tL = aggregatedData.reduce((s, d) => s + (d.leads || 0), 0);
    const t7 = aggregatedData.reduce((s, d) => s + (d.leads_7d || 0), 0);
    const doneC = aggregatedData.filter(d => d.status === 'done').length;
    return { tL, t7, doneC, m: aggregatedData.length, rawCities: filteredData.length };
  }, [aggregatedData, filteredData]);

  const barChartData = useMemo(() => {
    const top = aggregatedData.slice(0, 15);
    return {
      labels: top.map(d => d.label),
      datasets: [{
        data: top.map(d => d.leads),
        backgroundColor: top.map(d => (RC[d.region] || '#6b7280') + 'b3'),
        borderRadius: 4,
        borderWidth: 0
      }]
    };
  }, [aggregatedData]);

  const pieDataObj = useMemo(() => {
    const map = {};
    aggregatedData.forEach(d => { map[d.region] = (map[d.region] || 0) + (d.leads || 0); });
    const entries = Object.entries(map).sort((a,b)=>b[1]-a[1]);
    return {
      labels: entries.map(kv => kv[0]),
      entries,
      total: entries.reduce((s, kv) => s + kv[1], 0),
      datasets: [{
        data: entries.map(kv => kv[1]),
        backgroundColor: entries.map(kv => RC[kv[0]] || '#6b7280'),
        borderWidth: 2, borderColor: '#0d0f12'
      }]
    };
  }, [aggregatedData]);

  const renderTab1 = () => (
    <>
      <div className="metrics">
        <div className="mc a"><div className="mlb">Total Leads</div><div className="mv">{fmt(tab1Stats.tL)}</div><div className="ms">all markets</div></div>
        <div className="mc am"><div className="mlb">+7d Leads</div><div className="mv">{tab1Stats.t7>0?'+':''}{fmt(tab1Stats.t7)}</div><div className="ms">last 7 days</div></div>
        <div className="mc"><div className="mlb">Datapoints</div><div className="mv">{viewMode==='city'?tab1Stats.m:tab1Stats.m+' / '+tab1Stats.rawCities}</div><div className="ms">{viewMode==='city'?'unique cities':'countries / cities'}</div></div>
        <div className="mc g"><div className="mlb">Completed</div><div className="mv">{tab1Stats.doneC}</div><div className="ms">status: done</div></div>
        <div className="mc"><div className="mlb">Upcoming</div><div className="mv">{tab1Stats.m - tab1Stats.doneC}</div><div className="ms">status: new</div></div>
      </div>
      <div className="charts-row">
        <div className="cc" style={{flex: 2}}>
          <div className="ct">Leads by {viewMode === 'city' ? 'Market' : 'Country'} — Top 15</div>
          <div style={{position:'relative', height: Math.max(200, Math.min(15, aggregatedData.length)*32+40)}}>
            <Bar options={{ indexAxis: 'y', responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { grid: { color: 'rgba(255,255,255,.05)' }, ticks: { color: '#9ca3af', font: {size: 11} } }, y: { grid: { display: false }, ticks: { color: '#e5e7eb', font: {size: 11} } } } }} data={barChartData} />
          </div>
        </div>
        <div className="cc" style={{flex: 1}}>
          <div className="ct">Performance by Region</div>
          <div style={{position:'relative', height:'180px'}}><Doughnut options={{ responsive: true, maintainAspectRatio: false, cutout: '65%', plugins: { legend: { display: false } } }} data={pieDataObj} /></div>
          <div className="pie-legend" style={{marginTop:'15px', display:'flex', flexDirection:'column', gap:'8px'}}>
            {pieDataObj.entries.slice(0, 5).map(([r,v]) => (
              <div key={r} style={{display:'flex', alignItems:'center', justifyContent:'space-between', fontSize:'12px'}}><div style={{display:'flex', alignItems:'center', gap:'8px'}}><div style={{width:'10px', height:'10px', borderRadius:'50%', background:RC[r]||'#6b7280'}}></div><span style={{color:'#d1d5db'}}>{r}</span></div><div style={{display:'flex', alignItems:'center', gap:'12px'}}><span style={{color:'#9ca3af', fontFamily:'var(--mo)'}}>{fmt(v)}</span><span style={{color:'#6b7280', fontFamily:'var(--mo)', width:'30px', textAlign:'right'}}>{pieDataObj.total>0?Math.round(v/pieDataObj.total*100):0}%</span></div></div>
            ))}
          </div>
        </div>
      </div>
      <div className="tcard">
        <div className="thr"><div className="tt">Global Leads Database</div></div>
        <div className="tscroll">
          <table>
            <thead><tr><th style={{width: '30%'}}>{viewMode === 'city' ? 'City & Country' : 'Country'}</th><th style={{width: '20%'}}>Region</th><th style={{textAlign: 'right'}}>Total Leads</th><th style={{textAlign: 'right', color: '#34d399'}}>+7d New</th><th style={{textAlign: 'center'}}>Market Share</th><th style={{textAlign: 'center'}}>Status</th></tr></thead>
            <tbody>
              {aggregatedData.map(d => { const pct = tab1Stats.tL>0 ? Math.round((d.leads||0)/tab1Stats.tL*100) : 0; return (<tr key={d.id}><td style={{fontWeight: 500, color: '#e5e7eb'}}>{d.label}{viewMode === 'country' && <span style={{marginLeft:'8px', fontSize:'11px', color:'#9ca3af'}}>({d.cities} cities)</span>}</td><td><span style={{color: RC[d.region]||'#9ca3af', fontSize:'11px', padding:'2px 8px', background:'rgba(255,255,255,0.05)', borderRadius:'4px'}}>{d.region}</span></td><td style={{fontFamily:'var(--mo)', fontWeight:600, textAlign: 'right', color: '#f3f4f6'}}>{fmt(d.leads)}</td><td style={{fontFamily:'var(--mo)', textAlign: 'right', color: '#34d399'}}>{d.leads_7d > 0 ? `+${fmt(d.leads_7d)}` : '—'}</td><td><div className="bcel" style={{justifyContent: 'center'}}><div className="bbg"><div className="bfill" style={{width:`${Math.min(100, Math.max(2, (d.leads||0)/Math.max(1, aggregatedData[0]?.leads||1)*100))}%`, background:RC[d.region]||'#6b7280'}}></div></div><span className="bpct">{pct}%</span></div></td><td style={{textAlign: 'center'}}><span className={`spill ${d.status==="done"?'done':'upcoming'}`}>{d.status==="done" ? "✓ Done" : "○ Active"}</span></td></tr>); })}
            </tbody>
          </table>
          {aggregatedData.length === 0 && <div className="empty">No data points present.</div>}
        </div>
      </div>
    </>
  );

  const renderTab2 = () => {
    const sortedC = [...campaignData].sort((a,b)=>b.total_leads-a.total_leads);
    const topCol = ['#3b82f6','#8b5cf6','#ec4899','#f59e0b','#10b981'];
    return (
      <>
        <div className="charts-row"><div className="cc" style={{flex: 1}}><div className="ct">Top Traffic Sources</div><div style={{position:'relative', height: '280px'}}><Bar options={{ indexAxis: 'y', responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { grid: { color: 'rgba(255,255,255,.05)' }, ticks: { color: '#9ca3af' } }, y: { grid: { display: false }, ticks: { color: '#e5e7eb', font: {size: 11} } } } }} data={{ labels: sortedC.slice(0, 10).map(d => d.source.length > 20 ? d.source.slice(0,18)+'...' : d.source), datasets: [{ data: sortedC.slice(0, 10).map(d => d.total_leads), backgroundColor: sortedC.slice(0, 10).map((_, i) => topCol[i%topCol.length]), borderRadius: 4 }] }} /></div></div></div>
        <div className="tcard">
          <div className="thr"><div className="tt">Campaigns Pipeline</div></div>
          <div className="tscroll">
            <table>
              <thead><tr><th style={{width: '40%'}}>Traffic Source</th><th style={{textAlign: 'right'}}>Total Leads (Volume)</th><th style={{textAlign: 'right', color:'#34d399'}}>New (7d)</th><th style={{textAlign: 'right'}}>Qualified Buyers</th><th style={{textAlign: 'center'}}>Conv. Rate</th></tr></thead>
              <tbody>
                {sortedC.map((d, i) => (<tr key={d.id}><td><strong style={{color:'#e5e7eb', fontWeight: 500}}>{d.source}</strong>{i < 3 && <span style={{marginLeft:'10px', fontSize:'9px', padding:'2px 4px', background:'#3b82f633', color:'#60a5fa', borderRadius:'4px'}}>TOP {i+1}</span>}</td><td style={{fontFamily:'var(--mo)', textAlign: 'right', fontWeight: 600}}>{fmt(d.total_leads)}</td><td style={{fontFamily:'var(--mo)', color:'#34d399', textAlign: 'right'}}>{d.new_leads_7d > 0 ? `+${fmt(d.new_leads_7d)}` : '—'}</td><td style={{fontFamily:'var(--mo)', textAlign: 'right', color: d.buyers > 0 ? '#fcd34d' : '#9ca3af'}}>{fmt(d.buyers)}</td><td style={{textAlign: 'center'}}><span style={{fontFamily:'var(--mo)', fontWeight: 600, color: d.conversion >= 2 ? '#a78bfa' : '#6b7280', fontSize:'13px'}}>{d.conversion}%</span></td></tr>))}
              </tbody>
            </table>
          </div>
        </div>
      </>
    );
  };

  const renderTab3 = () => (
    <div className="tcard">
      <div className="thr"><div className="tt">Systems & Import Logistics</div></div>
      <div className="tscroll">
        <table>
          <thead><tr><th>Log #</th><th>Timestamp</th><th>Import Context</th><th style={{textAlign: 'right'}}>Total Scan</th><th style={{textAlign: 'right', color:'#34d399'}}>Inserted (+New)</th><th style={{textAlign: 'right', color:'#fbbf24'}}>Updated (Merge)</th><th style={{textAlign: 'right', color:'#ef4444'}}>Skipped (Dups/Err)</th></tr></thead>
          <tbody>
            {importData.length === 0 ? <tr><td colSpan="7" className="empty">No import activity logged yet.</td></tr> : importData.map(d => (<tr key={d.id}><td style={{fontFamily:'var(--mo)', color:'#9ca3af', fontSize:'11px'}}>#{String(d.id).padStart(4, '0')}</td><td style={{fontFamily:'var(--mo)', fontSize:'12px', color:'#d1d5db'}}>{d.created_at}</td><td><div style={{color:'#e5e7eb', fontWeight:500}}>{d.filename}</div><div style={{color:'#6b7280', fontSize:'11px', marginTop:'2px'}}>Via: {d.source}</div></td><td style={{fontFamily:'var(--mo)', textAlign: 'right', color:'#f3f4f6'}}>{fmt(d.total_rows)}</td><td style={{fontFamily:'var(--mo)', color:'#34d399', textAlign: 'right', fontWeight: d.inserted > 0 ? 600 : 400}}>{fmt(d.inserted)}</td><td style={{fontFamily:'var(--mo)', color:'#fbbf24', textAlign: 'right', fontWeight: d.updated > 0 ? 600 : 400}}>{fmt(d.updated)}</td><td style={{fontFamily:'var(--mo)', color:'#ef4444', textAlign: 'right', fontWeight: d.skipped > 0 ? 600 : 400}}>{fmt(d.skipped)}</td></tr>))}
          </tbody>
        </table>
      </div>
    </div>
  );

  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      <div className="topbar">
        <div className="logo"><b style={{color: '#fff'}}>LEADS IMPORTER</b> <span style={{color: '#6b7280'}}>• Analytics Core</span></div>
        <div className="hacts">
          {message && <span style={{marginRight:'15px', fontSize:'12px', color: message.type==='error'?'#ef4444':'#add6ff'}}>{message.text}</span>}
          <button className="ref-btn" onClick={fetchData} style={{background:'transparent', border:'1px solid #374151', color:'#9ca3af', padding:'5px 12px', borderRadius:'4px', marginRight:'10px', cursor:'pointer'}}>↻ Refresh</button>
          <label className="ulbl" style={{background:'#10b981', color:'#fff', padding:'5px 15px', borderRadius:'4px', cursor: importing ? 'not-allowed' : 'pointer', fontSize:'13px', fontWeight:600, marginRight:'10px'}}>{importing ? "Processing..." : "Import Folder"}<input type="file" webkitdirectory="" directory="" multiple onChange={handleFolderUpload} disabled={importing} style={{display:'none'}} /></label>
          <label className="ulbl" style={{background:'#3b82f6', color:'#fff', padding:'5px 15px', borderRadius:'4px', cursor: importing ? 'not-allowed' : 'pointer', fontSize:'13px', fontWeight:600}}>{importing ? "Processing..." : "Import CSV"}<input type="file" onChange={handleFileUpload} disabled={importing} style={{display:'none'}} /></label>
        </div>
      </div>
      <div className="page" style={{ display: 'flex', flex: 1 }}>
        <div className="sidebar">
          <div>
            <div className="sl">Main Menu</div>
            <div style={{display:'flex', flexDirection:'column', gap:'5px', marginTop:'10px'}}>
              <button className="fbtn active" style={{justifyContent: 'flex-start', borderLeft: '3px solid #3b82f6'}}>
                <span className="rdot" style={{marginRight:'8px'}}></span>Overview
              </button>
              <button className="fbtn" onClick={() => window.location.href = '/analytics'} style={{justifyContent: 'flex-start', borderLeft: '3px solid transparent'}}>
                <span className="rdot" style={{marginRight:'8px'}}></span>BI Analytics
              </button>
            </div>
          </div>
          <div className="dvd"></div>
          <div><div className="sl">Dashboards</div><div style={{display:'flex', flexDirection:'column', gap:'5px', marginTop:'10px'}}>{availableTabs.map(tab => (<button key={tab.id} className={`fbtn ${activeTab === tab.id ? 'active' : ''}`} onClick={() => setActiveTab(tab.id)} style={{justifyContent: 'flex-start', borderLeft: activeTab===tab.id?'3px solid #3b82f6':'3px solid transparent'}}><span className="rdot" style={{background: activeTab === tab.id ? '#3b82f6' : '#4b5563', marginRight:'8px'}}></span>{tab.title}</button>))}</div></div>
          <div className="dvd"></div>
          <div><div className="sl">Session Role</div><select value={userRole} onChange={e => setUserRole(e.target.value)} style={{width: '100%', marginTop:'8px', background: '#111827', color: '#e5e7eb', border: '1px solid #374151', padding: '8px', borderRadius: '6px', cursor: 'pointer', fontFamily:'var(--fn)'}}><option value="admin">Administrator (All Access)</option><option value="manager">Manager (Metrics & Sources)</option><option value="viewer">Viewer (Metrics Only)</option></select></div>
          {activeTab === 15 && (<><div className="dvd"></div><div><div className="sl" style={{marginBottom:'10px'}}>Data Granularity</div><div style={{display:'flex', gap:'5px', background:'#111827', border:'1px solid #374151', borderRadius:'6px', padding:'4px', overflow:'hidden'}}><button onClick={() => setViewMode('country')} style={{flex: 1, padding:'6px', background: viewMode==='country'?'#3b82f6':'transparent', border:'none', color:viewMode==='country'?'#fff':'#9ca3af', borderRadius:'4px', cursor:'pointer', fontFamily:'var(--fn)', fontSize:'12px', fontWeight:viewMode==='country'?600:400}}>by Country</button><button onClick={() => setViewMode('city')} style={{flex: 1, padding:'6px', background: viewMode==='city'?'#3b82f6':'transparent', border:'none', color:viewMode==='city'?'#fff':'#9ca3af', borderRadius:'4px', cursor:'pointer', fontFamily:'var(--fn)', fontSize:'12px', fontWeight:viewMode==='city'?600:400}}>by City</button></div></div><div className="dvd"></div><div><div className="sl" style={{marginBottom:'10px'}}>Search Filters</div><input type="text" placeholder="Type city or country..." value={search} onChange={e=>setSearch(e.target.value)} style={{width:'100%', background:'#111827', border:'1px solid #374151', borderRadius:'6px', padding:'8px 12px', color:'#f3f4f6', fontFamily:'var(--fn)', fontSize:'12px'}} /></div></>)}
          <div className="dvd"></div>
          <div className="sidebar-footer">
            <div className="sl">System Link</div>
            <a href="/nocodb/" target="_blank" rel="noreferrer" className="fbtn" style={{textDecoration:'none'}}>
              <span className="rdot" style={{marginRight:'8px', background:'#10b981'}}></span>NocoDB Tables
            </a>
          </div>
        </div>
        <div className="content">{loading ? (<div style={{display:'flex', justifyContent:'center', alignItems:'center', height:'50vh', color:'#9ca3af'}}><div style={{fontSize: '18px', fontWeight: 500}}>Syncing Core DB...</div></div>) : (<>{activeTab === 15 && renderTab1()}{activeTab === 16 && renderTab2()}{activeTab === 17 && renderTab3()}</>)}</div>
      </div>
    </div>
  );
};

export default LeadsDashboard;
