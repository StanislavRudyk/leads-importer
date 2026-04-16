import React, { useState, useEffect, useMemo } from 'react';
import {
  Chart as ChartJS, CategoryScale, LinearScale,
  BarElement, ArcElement, Title, Tooltip, Legend
} from 'chart.js';
import { Bar, Doughnut } from 'react-chartjs-2';
import { MapContainer, TileLayer, CircleMarker, Popup, Polyline, Tooltip as LeafletTooltip } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';

ChartJS.register(CategoryScale, LinearScale, BarElement, ArcElement, Title, Tooltip, Legend);

const RC = {
  'USA': '#3b82f6',      // Blue
  'Europe': '#10b981',   // Emerald
  'CIS': '#8b5cf6',      // Violet
  'Asia': '#f59e0b',     // Amber
  'LatAm': '#ec4899',    // Pink
  'Other': '#64748b'     // Slate
};
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
  const [activeTasks, setActiveTasks] = useState([]);
  const [activeCount, setActiveCount] = useState(0);
  const [showQueue, setShowQueue] = useState(false);
  const folderInputRef = React.useRef(null);
  const fileInputRef = React.useRef(null);

  const [data, setData] = useState([]);
  const [search, setSearch] = useState('');
  const [fReg, setFReg] = useState('all');
  const [viewMode, setViewMode] = useState('city');
  const [mapData, setMapData] = useState([]);

  // Sorting state
  const [sortConfig, setSortConfig] = useState({ key: 'leads', direction: 'desc' });
  // Inline edit state (Read/Write)
  const [editingRow, setEditingRow] = useState(null);
  const [editValues, setEditValues] = useState({});

  const [campaignData, setCampaignData] = useState([]);
  const [importData, setImportData] = useState([]);
  const [tab1Metrics, setTab1Metrics] = useState(null);
  const [usStats, setUsStats] = useState([]);
  
  // New Sidebar filters
  const [statusFilter, setStatusFilter] = useState('all');
  const [minLeads, setMinLeads] = useState(0);

  const regionStats = useMemo(() => {
    const stats = {};
    data.forEach(d => {
      stats[d.region] = (stats[d.region] || 0) + (d.leads || 0);
    });
    return stats;
  }, [data]);

  const statusStats = useMemo(() => {
    const stats = { all: data.length, upcoming: 0, completed: 0 };
    data.forEach(d => {
      if (d.status === 'done') stats.completed++;
      else stats.upcoming++;
    });
    return stats;
  }, [data]);

  const fetchData = async (isBackground = false) => {
    if (!isBackground) setLoading(true);
    try {
      const msR = await fetch(`${API_BASE}/api/v1/dashboard/metrics`);
      if (msR.ok) {
          const msD = await msR.json();
          setTab1Metrics(msD);
      }
      

      if (activeTab === 15) {
        const r = await fetch(`${API_BASE}/api/v1/dashboard/overview`);
        if (!r.ok) throw new Error("API Error");
        const d = await r.json();
        setData(Array.isArray(d) ? d : (d.items || []));
        
        const mr = await fetch(`${API_BASE}/api/v1/dashboard/map-data`);
        if (mr.ok) {
            setMapData(await mr.json());
        }
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
      } else if (activeTab === 18) {
        const r = await fetch(`${API_BASE}/api/v1/dashboard/us-stats`);
        if (r.ok) setUsStats(await r.json());
      }

      // Fetch active tasks
      const activeR = await fetch(`${API_BASE}/api/v1/import/active`);
      if (activeR.ok) {
        const activeD = await activeR.json();
        setActiveTasks(activeD.tasks || []);
        setActiveCount(activeD.count || 0);
      }
    } catch (e) {
      console.error("Fetch error:", e);
      // Если сервер не отвечает, показываем понятную ошибку вместо пустого экрана
      if (message === null) {
          setMessage({ text: "Server Busy. Re-syncing in 5s...", type: "error" });
      }
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
    if (userRole === 'admin') return [
      {id: 15, title: 'Leads Overview'}, 
      {id: 18, title: 'US States Analytics'},
      {id: 16, title: 'Campaigns & Sources'}, 
      {id: 17, title: 'System & Imports'}
    ];
    if (userRole === 'manager') return [
      {id: 15, title: 'Leads Overview'}, 
      {id: 18, title: 'US States Analytics'},
      {id: 16, title: 'Campaigns & Sources'}
    ];
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
    
    // Execute pool with thundering herd prevention
    const queue = [...files];
    const workers = Array(Math.min(concurrencyLimit, queue.length)).fill(null).map(async () => {
        while (queue.length > 0) {
            const nextFile = queue.shift();
            if (nextFile) {
                await processFile(nextFile);
                // Throttling to prevent slamming the server
                await new Promise(r => setTimeout(r, 100)); 
            }
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
    if (statusFilter !== 'all') {
      if (statusFilter === 'completed') res = res.filter(d => d.status === 'done');
      else res = res.filter(d => d.status !== 'done');
    }
    if (minLeads > 0) res = res.filter(d => (d.leads || 0) >= minLeads);
    return res;
  }, [data, search, fReg, statusFilter, minLeads]);

  const requestSort = (key) => {
    let direction = 'desc';
    if (sortConfig.key === key && sortConfig.direction === 'desc') direction = 'asc';
    setSortConfig({ key, direction });
  };

  const startEdit = (row) => {
    setEditingRow(row.id);
    setEditValues({ 
        status: row.status || 'soon', 
        spent: row.spent || 0, 
        notes: row.notes || '', 
        impressions: row.impressions || 0,
        show_context: row.show_context || '',
        state: row.state || '',
        city: row.city || ''
    });
  };

  const saveEdit = async (row) => {
    try {
      // 1. Update Market stats (Spent, Status, etc)
      const marketPayload = {
        city: row.raw_city || row.city, country_iso2: row.country,
        status: editValues.status, spent: parseFloat(editValues.spent) || 0,
        notes: editValues.notes, impressions: parseInt(editValues.impressions) || 0
      };
      
      const rMarket = await fetch(`${API_BASE}/api/v1/markets`, {
        method: "PATCH", headers: { "Content-Type": "application/json", "API-Key": API_KEY },
        body: JSON.stringify(marketPayload)
      });

      // 2. Update Lead info (Artist, State, City) via Bulk Update
      const bulkPayload = {
        old_city: row.raw_city || row.city,
        old_country: row.country,
        old_show_context: row.show_context,
        city: editValues.city,
        state: editValues.state,
        show_context: editValues.show_context
      };

      const rBulk = await fetch(`${API_BASE}/api/v1/dashboard/bulk-update`, {
        method: "PATCH", headers: { "Content-Type": "application/json", "API-Key": API_KEY },
        body: JSON.stringify(bulkPayload)
      });

      if (rMarket.ok && rBulk.ok) {
        setData(prev => prev.map(d => d.id === row.id ? { 
            ...d, ...marketPayload, 
            city: editValues.city, state: editValues.state, show_context: editValues.show_context,
            cpl: marketPayload.spent / (d.leads || 1) 
        } : d));
        setEditingRow(null);
        setMessage({ text: "Global Update Success!", type: "success" });
        setTimeout(() => setMessage(null), 2000);
      } else {
        setMessage({ text: "Update failed.", type: "error" });
        setTimeout(() => setMessage(null), 3000);
      }
    } catch (e) { console.error(e); }
  };

  const handleDeleteMarket = async (row) => {
    if (!window.confirm(`Delete financial data for ${row.city}? Leads will remain in DB.`)) return;
    try {
      const r = await fetch(`${API_BASE}/api/v1/markets?city=${encodeURIComponent(row.raw_city || row.city)}&country_iso2=${encodeURIComponent(row.country)}`, {
        method: "DELETE", headers: { "API-Key": API_KEY }
      });
      if (r.ok) {
        setData(prev => prev.map(d => d.id === row.id ? { ...d, spent: 0, cpl: 0, status: 'soon', notes: '', impressions: 0 } : d));
        setMessage({ text: "Market data reset.", type: "success" });
        setTimeout(() => setMessage(null), 2000);
      }
    } catch (e) { console.error(e); }
  };



  const aggregatedData = useMemo(() => {
    let agg;
    if (viewMode === 'city') {
      agg = filteredData.map(d => {
        let label = d.city || d.country;
        if (d.country === 'US' && d.city && d.state) {
            label = `${d.city}, ${d.state}`;
        } else if (d.city) {
            label = `${d.city} / ${d.country}`;
        }
        return { ...d, label };
      });
    } else {
      const grouped = {};
      filteredData.forEach(d => {
        const cCode = d.country || "XX";
        if (!grouped[cCode]) grouped[cCode] = { id: cCode, label: cCode, country: cCode, region: d.region, leads: 0, leads_7d: 0, status: 'upcoming', cities: 0, spent: 0 };
        grouped[cCode].leads += (d.leads || 0);
        grouped[cCode].leads_7d += (d.leads_7d || 0);
        grouped[cCode].spent += (d.spent || 0);
        grouped[cCode].cities += 1;
        if (d.status === 'done') grouped[cCode].status = 'done';
      });
      agg = Object.values(grouped);
    }
    // Apply sorting
    agg.sort((a, b) => {
      let valA = a[sortConfig.key], valB = b[sortConfig.key];
      
      // Handle nulls
      if (valA == null) return sortConfig.direction === 'asc' ? 1 : -1;
      if (valB == null) return sortConfig.direction === 'asc' ? -1 : 1;

      // Robust alphanumeric comparison
      if (typeof valA === 'string' && typeof valB === 'string') {
        const res = valA.localeCompare(valB, undefined, { numeric: true, sensitivity: 'base' });
        return sortConfig.direction === 'asc' ? res : -res;
      }
      
      if (valA < valB) return sortConfig.direction === 'asc' ? -1 : 1;
      if (valA > valB) return sortConfig.direction === 'asc' ? 1 : -1;
      return 0;
    });
    return agg;
  }, [filteredData, viewMode, sortConfig]);

  const tab1Stats = useMemo(() => {
    const tL = tab1Metrics ? tab1Metrics.totalLeads : aggregatedData.reduce((s, d) => s + (d.leads || 0), 0);
    const t7 = tab1Metrics ? tab1Metrics.leads7d : aggregatedData.reduce((s, d) => s + (d.leads_7d || 0), 0);
    const doneC = tab1Metrics ? tab1Metrics.completed : aggregatedData.filter(d => d.status === 'done').length;
    return { tL, t7, doneC, m: aggregatedData.length, rawCities: filteredData.length };
  }, [aggregatedData, filteredData, tab1Metrics]);

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

  const renderTabMap = () => {
    // Group map data by artist for path visualization
    const artistPaths = {};
    const artistTotals = {};

    mapData.forEach(d => {
        if (!d.show_context) return;
        artistTotals[d.show_context] = (artistTotals[d.show_context] || 0) + (d.lead_count || 0);
        
        if (!d.latitude || !d.longitude) return;
        if (!artistPaths[d.show_context]) artistPaths[d.show_context] = [];
        artistPaths[d.show_context].push(d);
    });

    const topArtists = Object.entries(artistTotals).sort((a,b) => b[1]-a[1]).slice(0, 10);

    Object.keys(artistPaths).forEach(artist => {
        artistPaths[artist].sort((a,b) => (a.start_date||'').localeCompare(b.start_date||''));
    });

    return (
    <div style={{ height: 'calc(100vh - 80px)', width: '100%', display: 'flex', position: 'relative', overflow: 'hidden' }}>
        <div style={{ width: '220px', background: '#0f172a', borderRight: '1px solid #1e293b', padding: '12px', overflowY: 'auto', zIndex: 1000, display: 'flex', flexDirection: 'column' }}>
            <div style={{ color: '#fff', fontSize: '13px', fontWeight: 600, marginBottom: '12px' }}>Top Performing Artists</div>
            <div style={{ flex: 1 }}>
                {topArtists.map(([artist, count], i) => (
                    <div key={i} style={{ padding: '8px', background: i === 0 ? '#3b82f622' : 'transparent', borderRadius: '6px', marginBottom: '6px', border: '1px solid #1e293b' }}>
                        <div style={{ color: '#e2e8f0', fontSize: '12px', fontWeight: 500, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{artist}</div>
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: '4px' }}>
                            <span style={{ color: '#3b82f6', fontSize: '11px', fontWeight: 600 }}>{fmt(count)} Leads</span>
                            <span style={{ color: '#64748b', fontSize: '10px' }}>#{i+1}</span>
                        </div>
                    </div>
                ))}
            </div>

            <div style={{ marginTop: '15px', background: 'rgba(15, 23, 42, 0.9)', padding: '10px', borderRadius: '8px', border: '1px solid #334155', color: '#e5e7eb', fontSize: '11px' }}>
                <div style={{ fontWeight: 600, marginBottom: '8px' }}>Map Legend</div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '4px' }}><div style={{width: 8, height: 8, borderRadius: '50%', background: '#10b981'}}></div> Active Show</div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '4px' }}><div style={{width: 8, height: 8, borderRadius: '50%', background: '#6b7280'}}></div> Completed</div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}><div style={{width: 8, height: 8, borderRadius: '50%', background: '#3b82f6'}}></div> Upcoming / Soon</div>
                <div style={{ height: '1px', background: '#334155', margin: '8px 0' }}></div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: '#94a3b8' }}><div style={{width: 15, height: 1, borderTop: '1px dashed #3b82f6'}}></div> Artist Path</div>
            </div>
        </div>
        <div style={{ flex: 1, position: 'relative' }}>
        <MapContainer center={[20, 0]} zoom={2} minZoom={2} maxBounds={[[-90, -180], [90, 180]]} maxBoundsViscosity={1.0} style={{ height: '100%', width: '100%', background: '#0e0e0e' }}>
            <TileLayer
                url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
                attribution='&copy; CARTO'
                noWrap={true}
                bounds={[[-90, -180], [90, 180]]}
            />
            {/* Draw Paths between shows of the same artist */}
            {Object.entries(artistPaths).map(([artist, points], groupIdx) => {
                if (points.length < 2) return null;
                const positions = points.map(p => [p.latitude, p.longitude]);
                return (
                    <Polyline 
                        key={`path-${groupIdx}`} 
                        positions={positions} 
                        pathOptions={{ color: '#3b82f6', weight: 1, dashArray: '5, 10', opacity: 0.4 }} 
                    />
                );
            })}

            {mapData.map((d, i) => {
                if (!d.latitude || !d.longitude) return null;
                const radius = Math.max(4, Math.min(25, Math.sqrt(d.lead_count) * 2));
                const color = d.show_context === 'active' ? '#10b981' : (d.show_context==='done' ? '#6b7280' : '#3b82f6');
                return (
                    <CircleMarker 
                        key={i} 
                        center={[d.latitude, d.longitude]} 
                        radius={radius}
                        pathOptions={{ color: color, fillColor: color, fillOpacity: 0.6, weight: 1 }}
                    >
                        <Popup>
                            <div style={{color:'#000', fontFamily:'Inter'}}>
                                <strong style={{fontSize: '14px'}}>{d.city}, {d.country_iso2}</strong><br/>
                                <span style={{fontSize: '12px'}}>Artist: {d.show_context || 'N/A'}</span><br/>
                                <span style={{fontSize: '12px', fontWeight: 600, color: '#2563eb'}}>Leads: {d.lead_count}</span><br/>
                                <span style={{fontSize: '11px', color: '#666', marginTop: '5px', display: 'block'}}>
                                  {d.start_date ? new Date(d.start_date).toLocaleDateString() : 'N/A'} - {d.end_date ? new Date(d.end_date).toLocaleDateString() : 'N/A'}
                                </span>
                            </div>
                        </Popup>
                    </CircleMarker>
                );
            })}
        </MapContainer>
        </div>
    </div>
    );
  };

  const renderTab1 = () => {
    if (viewMode === 'map') return renderTabMap();
    return (
    <>
      <div className="metrics">
        <div className="mc"><div className="mlb">Total Leads</div><div className="mv">{fmt(tab1Stats.tL)}</div><div className="ms">all markets</div></div>
        <div className="mc"><div className="mlb">+7d Leads</div><div className="mv">{tab1Stats.t7>0?'+':''}{fmt(tab1Stats.t7)}</div><div className="ms">last 7 days</div></div>
        <div className="mc"><div className="mlb">Datapoints</div><div className="mv">{viewMode==='city'?tab1Stats.m:tab1Stats.m+' / '+tab1Stats.rawCities}</div><div className="ms">{viewMode==='city'?'unique cities':'countries / cities'}</div></div>
        <div className="mc"><div className="mlb">Completed</div><div className="mv">{tab1Stats.doneC}</div><div className="ms">status: done</div></div>
        <div className="mc"><div className="mlb">Upcoming</div><div className="mv">{fmt(tab1Stats.tL - tab1Stats.doneC)}</div><div className="ms">status: new</div></div>
      </div>


      <div className="charts-row">
        <div className="cc" style={{flex: 5}}>
          <div className="ct">Leads by {viewMode === 'city' ? 'Market' : 'Country'} — Top 15</div>
          <div style={{position:'relative', height: Math.max(200, Math.min(15, aggregatedData.length)*32+40)}}>
            <Bar options={{ indexAxis: 'y', responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { grid: { color: 'rgba(255,255,255,.05)' }, ticks: { color: '#9ca3af', font: {size: 11} } }, y: { grid: { display: false }, ticks: { color: '#e5e7eb', font: {size: 11} } } } }} data={barChartData} />
          </div>
        </div>
        <div className="cc" style={{flex: 1, minWidth: '180px', maxWidth: '250px'}}>
          <div className="ct">By Region</div>
          <div style={{position:'relative', height:'140px'}}><Doughnut options={{ responsive: true, maintainAspectRatio: false, cutout: '65%', plugins: { legend: { display: false } } }} data={pieDataObj} /></div>
          <div className="pie-legend" style={{marginTop:'15px', display:'flex', flexDirection:'column', gap:'8px'}}>
            {pieDataObj.entries.slice(0, 5).map(([r,v]) => (
              <div key={r} style={{display:'flex', alignItems:'center', justifyContent:'space-between', fontSize:'12px'}}><div style={{display:'flex', alignItems:'center', gap:'8px'}}><div style={{width:'10px', height:'10px', borderRadius:'50%', background:RC[r]||'#6b7280'}}></div><span style={{color:'#d1d5db'}}>{r}</span></div><div style={{display:'flex', alignItems:'center', gap:'12px'}}><span style={{color:'#9ca3af', fontFamily:'var(--mo)'}}>{fmt(v)}</span><span style={{color:'#6b7280', fontFamily:'var(--mo)', width:'30px', textAlign:'right'}}>{pieDataObj.total>0?Math.round(v/pieDataObj.total*100):0}%</span></div></div>
            ))}
          </div>
        </div>
      </div>
      <div className="tcard">
        <div className="thr"><div className="tt">Global Leads Database & Market Intelligence</div></div>
        <div className="tscroll">
          <table>
            <thead><tr>
              <th style={{width:'3%'}}>#</th>
              <th style={{cursor:'pointer'}} onClick={()=>requestSort('label')}>{viewMode === 'city' ? 'City' : 'Country'} {sortConfig.key==='label'?(sortConfig.direction==='asc'?'↑':'↓'):''}</th>
              <th style={{cursor:'pointer'}} onClick={()=>requestSort('state')}>State {sortConfig.key==='state'?(sortConfig.direction==='asc'?'↑':'↓'):''}</th>
              <th style={{cursor:'pointer'}} onClick={()=>requestSort('region')}>Region {sortConfig.key==='region'?(sortConfig.direction==='asc'?'↑':'↓'):''}</th>
              <th style={{cursor:'pointer'}} onClick={()=>requestSort('show_context')}>Artist/Show {sortConfig.key==='show_context'?(sortConfig.direction==='asc'?'↑':'↓'):''}</th>
              <th style={{textAlign:'right',cursor:'pointer'}} onClick={()=>requestSort('leads')}>Leads {sortConfig.key==='leads'?(sortConfig.direction==='asc'?'↑':'↓'):''}</th>
              <th style={{textAlign:'right',color:'#34d399',cursor:'pointer'}} onClick={()=>requestSort('leads_7d')}>+7d {sortConfig.key==='leads_7d'?(sortConfig.direction==='asc'?'↑':'↓'):''}</th>
              <th style={{textAlign:'right',cursor:'pointer'}} onClick={()=>requestSort('spent')}>Spent$ {sortConfig.key==='spent'?(sortConfig.direction==='asc'?'↑':'↓'):''}</th>
              <th style={{textAlign:'right',cursor:'pointer'}} onClick={()=>requestSort('cpl')}>CPL$ {sortConfig.key==='cpl'?(sortConfig.direction==='asc'?'↑':'↓'):''}</th>
              <th style={{textAlign:'right',cursor:'pointer'}} onClick={()=>requestSort('impressions')}>Impr. {sortConfig.key==='impressions'?(sortConfig.direction==='asc'?'↑':'↓'):''}</th>
              <th style={{textAlign:'center',cursor:'pointer'}} onClick={()=>requestSort('status')}>Status</th>
              <th style={{cursor:'pointer'}} onClick={()=>requestSort('start_date')}>Timeline {sortConfig.key==='start_date'?(sortConfig.direction==='asc'?'↑':'↓'):''}</th>
              {userRole !== 'viewer' && <th style={{textAlign:'center',width:'8%'}}>Actions</th>}
            </tr></thead>
            <tbody>
              {aggregatedData.map((d, idx) => { const pct = tab1Stats.tL>0 ? Math.round((d.leads||0)/tab1Stats.tL*100) : 0; const isEd = editingRow === d.id; return (<tr key={d.id} style={{background: isEd ? 'rgba(59,130,246,0.1)' : 'transparent', borderBottom: '1px solid #1e293b'}}>
                <td style={{color:'#6b7280',fontSize:'10px'}}>{idx+1}</td>
                <td style={{fontWeight: 500}}>
                   {isEd ? <input value={editValues.city} onChange={e=>setEditValues({...editValues, city: e.target.value})} style={{width:'90px',background:'#0f172a',color:'#fff',border:'1px solid #3b82f6',padding:'2px 4px',borderRadius:'4px',fontSize:'11px'}} /> : d.label}
                </td>
                <td style={{fontSize:'11px',color:'#94a3b8'}}>
                   {isEd ? <input value={editValues.state} onChange={e=>setEditValues({...editValues, state: e.target.value})} placeholder="ST" style={{width:'35px',background:'#0f172a',color:'#fff',border:'1px solid #3b82f6',padding:'2px 4px',borderRadius:'4px',fontSize:'11px'}} /> : (d.state || '—')}
                </td>
                <td><span style={{color: RC[d.region]||'#9ca3af', fontSize:'10px', padding:'1px 5px', background:'rgba(255,255,255,0.03)', borderRadius:'3px'}}>{d.region}</span></td>
                <td>
                   {isEd ? <input value={editValues.show_context} onChange={e=>setEditValues({...editValues, show_context: e.target.value})} placeholder="Artist..." style={{width:'100px',background:'#0f172a',color:'#fff',border:'1px solid #3b82f6',padding:'2px 4px',borderRadius:'4px',fontSize:'11px'}} /> : (d.show_context || '—')}
                </td>
                <td style={{fontFamily:'var(--mo)', fontWeight:600, textAlign: 'right', color: '#f3f4f6'}}>{fmt(d.leads)}</td>
                <td style={{fontFamily:'var(--mo)', textAlign: 'right', color: '#34d399'}}>{d.leads_7d > 0 ? `+${fmt(d.leads_7d)}` : '—'}</td>
                {isEd ? (<>
                  <td><input type="number" value={editValues.spent} onChange={e=>setEditValues({...editValues,spent:e.target.value})} style={{width:'60px',background:'#0f172a',color:'#fff',border:'1px solid #3b82f6',padding:'2px 4px',borderRadius:'4px',fontFamily:'var(--mo)',fontSize:'11px'}}/></td>
                  <td style={{fontFamily:'var(--mo)',textAlign:'right',color:'#a78bfa',fontSize:'11px'}}>${(parseFloat(editValues.spent||0)/(d.leads||1)).toFixed(2)}</td>
                  <td><input type="number" value={editValues.impressions} onChange={e=>setEditValues({...editValues,impressions:e.target.value})} style={{width:'70px',background:'#0f172a',color:'#fff',border:'1px solid #3b82f6',padding:'2px 4px',borderRadius:'4px',fontFamily:'var(--mo)',fontSize:'11px'}}/></td>
                  <td style={{textAlign:'center'}}><select value={editValues.status} onChange={e=>setEditValues({...editValues,status:e.target.value})} style={{background:'#0f172a',color:'#fff',border:'1px solid #3b82f6',padding:'2px 4px',borderRadius:'4px',fontSize:'11px'}}><option value="done">Done</option><option value="active">Active</option><option value="soon">Soon</option></select></td>
                  <td><div style={{fontSize:'9px',color:'#64748b'}}>Linked to file creation</div></td>
                  <td style={{textAlign:'center'}}>
                    <div style={{display:'flex', gap:'4px', justifyContent:'center'}}>
                      <button onClick={()=>saveEdit(d)} style={{background:'#10b981',color:'#fff',border:'none',padding:'2px 6px',borderRadius:'3px',cursor:'pointer',fontSize:'10px'}}>Save</button>
                      <button onClick={()=>setEditingRow(null)} style={{background:'#334155',color:'#fff',border:'none',padding:'2px 6px',borderRadius:'3px',cursor:'pointer',fontSize:'10px'}}>Esc</button>
                    </div>
                  </td>
                </>) : (<>
                  <td style={{fontFamily:'var(--mo)',textAlign:'right',color:'#fcd34d',fontSize:'11px'}}>${fmt(d.spent||0)}</td>
                  <td style={{fontFamily:'var(--mo)',textAlign:'right',color:'#a78bfa',fontSize:'11px'}}>${(d.cpl||0).toFixed(2)}</td>
                  <td style={{fontFamily:'var(--mo)',textAlign:'right',color:'#9ca3af',fontSize:'11px'}}>{d.impressions ? fmt(d.impressions) : '—'}</td>
                  <td style={{textAlign:'center'}}><span className={`spill ${d.status==="done"?'done':d.status==='active'?'active':'upcoming'}`} style={{fontSize:'10px'}}>{d.status==="done" ? "✓ Done" : d.status==="active"?"► Active":"○ Soon"}</span></td>
                  <td>
                    <div style={{color:'#94a3b8',fontSize:'10px',whiteSpace:'nowrap'}}>
                      {d.start_date ? new Date(d.start_date).toLocaleDateString('en',{month:'short',day:'2-digit'}) : '—'} 
                      {d.end_date && ` - ${new Date(d.end_date).toLocaleDateString('en',{month:'short',day:'2-digit'})}`}
                    </div>
                  </td>
                  {userRole !== 'viewer' && <td style={{textAlign:'center'}}>
                    <div style={{display:'flex', gap:'4px', justifyContent:'center'}}>
                      <button onClick={()=>startEdit(d)} style={{background:'rgba(59,130,246,0.1)',border:'1px solid #3b82f644',color:'#60a5fa',padding:'2px 6px',borderRadius:'3px',cursor:'pointer',fontSize:'10px'}}>Edit</button>
                      <button onClick={()=>handleDeleteMarket(d)} style={{background:'rgba(239,68,68,0.1)',border:'1px solid #ef444444',color:'#f87171',padding:'2px 6px',borderRadius:'3px',cursor:'pointer',fontSize:'10px'}}>✕</button>
                    </div>
                  </td>}
                </>)}
              </tr>); })}
            </tbody>
          </table>
          {aggregatedData.length === 0 && <div className="empty">No data points present.</div>}
        </div>
      </div>
    </>
    );
  };

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
            {importData.length === 0 ? <tr><td colSpan="7" className="empty">No import activity logged yet.</td></tr> : importData.map(d => {
              const rowStatus = d.status === 'error' ? 'failed' : (d.status === 'skipped' ? 'skipped' : 'success');
              const statusColor = { 'success': '#34d399', 'skipped': '#94a3b8', 'failed': '#ef4444' }[rowStatus];
              return (
                <tr key={d.id} style={{ opacity: rowStatus === 'skipped' ? 0.7 : 1 }}>
                  <td style={{fontFamily:'var(--mo)', color:'#9ca3af', fontSize:'11px'}}>#{String(d.id).padStart(4, '0')}</td>
                  <td style={{fontFamily:'var(--mo)', fontSize:'12px', color:'#d1d5db'}}>{d.created_at}</td>
                  <td>
                    <div style={{color:'#e5e7eb', fontWeight:500}}>{d.filename}</div>
                    <div style={{color:'#6b7280', fontSize:'11px', marginTop:'2px'}}>Via: {d.source}</div>
                  </td>
                  <td style={{fontFamily:'var(--mo)', textAlign: 'right', color:'#f3f4f6'}}>{fmt(d.total_rows)}</td>
                  <td style={{fontFamily:'var(--mo)', color:'#34d399', textAlign: 'right', fontWeight: d.inserted > 0 ? 600 : 400}}>{fmt(d.inserted)}</td>
                  <td style={{fontFamily:'var(--mo)', color:'#fbbf24', textAlign: 'right', fontWeight: d.updated > 0 ? 600 : 400}}>{fmt(d.updated)}</td>
                  <td style={{fontFamily:'var(--mo)', color: statusColor, textAlign: 'right', fontWeight: d.skipped > 0 ? 600 : 400}}>
                    {fmt(d.skipped)}
                    {rowStatus === 'skipped' && <span style={{fontSize:'9px', marginLeft:'5px'}}>(SYNCHRONIZED)</span>}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );

  const renderTabUS = () => (
    <div className="tcard" style={{marginTop: '0'}}>
      <div className="thr">
        <div className="tt">United States — Geo-Density & Show Distribution</div>
        <div style={{color: '#64748b', fontSize: '11px', marginTop: '2px'}}>Analyzing leads by US State with AI-extracted show context</div>
      </div>
      <div className="tscroll" style={{maxHeight: 'calc(100vh - 250px)'}}>
        <table>
          <thead>
            <tr>
              <th style={{width: '15%'}}>US State</th>
              <th style={{textAlign: 'right'}}>Leads (Vol)</th>
              <th style={{textAlign: 'right'}}>Shows</th>
              <th>Primary Artist Context / Tour Roster</th>
              <th style={{textAlign: 'right'}}>First Intake</th>
              <th style={{textAlign: 'right'}}>Last Sync</th>
            </tr>
          </thead>
          <tbody>
            {usStats.length === 0 ? <tr><td colSpan="6" className="empty">No US state data aggregated yet. Run enrichment to populate.</td></tr> : usStats.map((s, i) => (
              <tr key={i} style={{borderBottom: '1px solid #1e293b'}}>
                <td><strong style={{color: '#e2e8f0', fontSize: '13px'}}>{s.state}</strong></td>
                <td style={{fontFamily: 'var(--mo)', textAlign: 'right', color: '#60a5fa', fontWeight: 600}}>{fmt(s.lead_count)}</td>
                <td style={{fontFamily: 'var(--mo)', textAlign: 'right', color: '#94a3b8'}}>{s.unique_shows}</td>
                <td style={{fontSize: '11px', color: '#cbd5e1', maxWidth: '300px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap'}}>{s.shows_list || '—'}</td>
                <td style={{fontFamily: 'var(--mo)', textAlign: 'right', fontSize: '11px', color: '#64748b'}}>{s.earliest ? new Date(s.earliest).toLocaleDateString() : '—'}</td>
                <td style={{fontFamily: 'var(--mo)', textAlign: 'right', fontSize: '11px', color: '#64748b'}}>{s.latest ? new Date(s.latest).toLocaleDateString() : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      <div className="topbar">
        <div className="logo"><b>LEADS IMPORTER</b></div>
        
        <nav className="nav-tabs">
          {availableTabs.map(tab => (
            <button 
              key={tab.id} 
              className={`nav-tab ${activeTab === tab.id ? 'active' : ''}`} 
              onClick={() => setActiveTab(tab.id)}
            >
              {tab.title}
            </button>
          ))}
        </nav>

        <div className="hacts">
          {activeCount > 0 && (
            <div className="active-queue" style={{ position: 'relative', marginRight: '5px' }}>
              <button 
                onClick={() => setShowQueue(!showQueue)}
                style={{ background: '#1e293b', border: '1px solid #334155', color: '#60a5fa', padding: '5px 10px', borderRadius: '4px', cursor: 'pointer', fontSize: '11px', fontWeight: 600, display: 'flex', alignItems: 'center', gap: '6px' }}
              >
                <span style={{ width: '6px', height: '6px', background: '#3b82f6', borderRadius: '50%', animation: 'pulse 1.5s infinite' }}></span>
                {activeCount} Tasks
              </button>
              {showQueue && (
                <div className="queue-dropdown" style={{ position: 'absolute', top: '100%', right: 0, marginTop: '8px', background: '#0f172a', border: '1px solid #1e293b', borderRadius: '8px', padding: '12px', width: '280px', boxShadow: '0 10px 15px -3px rgba(0, 0, 0, 0.5)', zIndex: 100 }}>
                  <div style={{ color: '#94a3b8', fontSize: '11px', marginBottom: '8px', textTransform: 'uppercase' }}>Server Queue</div>
                  <div style={{ maxHeight: '200px', overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: '6px' }}>
                    {activeTasks.map(t => (
                      <div key={t.task_id} style={{ display: 'flex', flexDirection: 'column', gap: '2px', padding: '6px', background: '#1e293b66', borderRadius: '4px' }}>
                        <div style={{ color: '#e2e8f0', fontSize: '11px', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{t.filename}</div>
                        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                          <span style={{ color: t.status === 'running' ? '#3b82f6' : '#94a3b8', fontSize: '9px', fontWeight: 600 }}>{t.status.toUpperCase()}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
          {message && <span style={{marginRight:'10px', fontSize:'11px', color: '#add6ff'}}>{message.text}</span>}
          <button className="btn" onClick={fetchData}>↻ Refresh</button>
          <button className="btn btn-p" onClick={() => folderInputRef.current?.click()}>Import Folder</button>
          <button className="btn btn-p" onClick={() => fileInputRef.current?.click()}>Import CSV</button>
          
          <input type="file" ref={folderInputRef} webkitdirectory="true" directory="true" multiple onChange={handleFolderUpload} style={{display:'none'}} />
          <input type="file" ref={fileInputRef} onChange={handleFileUpload} style={{display:'none'}} />
        </div>
      </div>

      <div className="page">
        <aside className="sidebar">
          <div className="s-section">
            <div className="s-label">Search</div>
            <input 
              type="text" 
              placeholder="City or region..." 
              value={search} 
              onChange={e => setSearch(e.target.value)} 
            />
          </div>

          <div className="s-section">
            <div className="s-label">Region</div>
            <div className="s-list">
              <div 
                className={`s-item ${fReg === 'all' ? 'active' : ''}`} 
                onClick={() => setFReg('all')}
              >
                <span>All regions</span>
                <span className="count">{fmt(Object.values(regionStats).reduce((a,b)=>a+b, 0))}</span>
              </div>
              {Object.entries(RC).map(([reg, col]) => (
                regionStats[reg] > 0 && (
                  <div 
                    key={reg} 
                    className={`s-item ${fReg === reg ? 'active' : ''}`} 
                    onClick={() => setFReg(reg)}
                  >
                    <div style={{display:'flex', alignItems:'center'}}>
                      <div className="dot" style={{background: col}}></div>
                      <span>{reg}</span>
                    </div>
                    <span className="count">{fmt(regionStats[reg])}</span>
                  </div>
                )
              ))}
            </div>
          </div>

          <div className="s-section">
            <div className="s-label">Status</div>
            <div className="s-list">
              <div className={`s-item ${statusFilter === 'all' ? 'active' : ''}`} onClick={() => setStatusFilter('all')}>
                <span>All</span>
                <span className="count">{statusStats.all}</span>
              </div>
              <div className={`s-item ${statusFilter === 'upcoming' ? 'active' : ''}`} onClick={() => setStatusFilter('upcoming')}>
                <span>Upcoming</span>
                <span className="count">{statusStats.upcoming}</span>
              </div>
              <div className={`s-item ${statusFilter === 'completed' ? 'active' : ''}`} onClick={() => setStatusFilter('completed')}>
                <span>Completed</span>
                <span className="count">{statusStats.completed}</span>
              </div>
            </div>
          </div>

          <div className="s-section">
            <div className="s-label">Sort By</div>
            <select value={sortConfig.key} onChange={e => setSortConfig({...sortConfig, key: e.target.value})}>
              <option value="leads">Total Leads</option>
              <option value="leads_7d">New Leads (7d)</option>
              <option value="label">City/Location</option>
              <option value="state">US State</option>
              <option value="show_context">Artist Context</option>
              <option value="spent">Spent ($)</option>
              <option value="cpl">CPL ($)</option>
              <option value="start_date">Collection Date</option>
            </select>
          </div>

          <div className="s-section">
            <div className="s-label">Min Leads</div>
            <input 
              type="number" 
              value={minLeads} 
              onChange={e => setMinLeads(parseInt(e.target.value) || 0)} 
            />
          </div>

          <div className="dvd"></div>
          <div className="s-section">
            <div className="s-label">Session Role</div>
            <select value={userRole} onChange={e => setUserRole(e.target.value)}>
              <option value="admin">Administrator</option>
              <option value="manager">Manager</option>
              <option value="viewer">Viewer</option>
            </select>
          </div>
          
          <div className="sidebar-footer">
            <a href="/nocodb/" target="_blank" className="btn" style={{width:'100%', justifyContent:'center'}}>NocoDB Tables</a>
          </div>
        </aside>

        <main className="content">
          {loading ? (
            <div className="loading-container">Syncing Core DB...</div>
          ) : (
            <>
              {activeTab === 15 && (
                <>
                  <div className="granularity-row">
                    <div className="ct" style={{marginBottom: 0}}>Data Granularity</div>
                    <div className="dash-tabs">
                      <button className={`dash-tab ${viewMode==='country'?'active':''}`} onClick={()=>setViewMode('country')}>by Country</button>
                      <button className={`dash-tab ${viewMode==='city'?'active':''}`} onClick={()=>setViewMode('city')}>by City</button>
                      <button className={`dash-tab ${viewMode==='map'?'active':''}`} onClick={()=>setViewMode('map')}>Map</button>
                    </div>
                  </div>
                  {renderTab1()}
                </>
              )}
              {activeTab === 16 && renderTab2()}
              {activeTab === 17 && renderTab3()}
              {activeTab === 18 && renderTabUS()}
            </>
          )}
        </main>
      </div>
    </div>
  );
};

export default LeadsDashboard;
