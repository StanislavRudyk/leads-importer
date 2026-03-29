import React from 'react';
import { NavLink } from 'react-router-dom';

const Sidebar = () => {
  return (
    <div className="sidebar">
      <div>
        <div className="sl">Main Menu</div>
        <div style={{display:'flex', flexDirection:'column', gap:'5px', marginTop:'10px'}}>
          <NavLink
            to="/"
            className={({ isActive }) => isActive ? "fbtn active" : "fbtn"}
            style={({ isActive }) => ({
              justifyContent: 'flex-start',
              borderLeft: isActive ? '3px solid #3b82f6' : '3px solid transparent',
              textDecoration: 'none'
            })}
          >
            <span className="rdot" style={{marginRight:'8px'}}></span>
            Overview
          </NavLink>
          <NavLink
            to="/analytics"
            className={({ isActive }) => isActive ? "fbtn active" : "fbtn"}
            style={({ isActive }) => ({
              justifyContent: 'flex-start',
              borderLeft: isActive ? '3px solid #3b82f6' : '3px solid transparent',
              textDecoration: 'none'
            })}
          >
            <span className="rdot" style={{marginRight:'8px'}}></span>
            BI Analytics
          </NavLink>
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
  );
};

export default Sidebar;
