import React, { useState, useEffect } from "react";

const API_BASE = process.env.REACT_APP_API_URL || "http://localhost:8000";

export default function MetabaseDashboard({ dashboardId, userRole = "viewer" }) {
  const [iframeUrl, setIframeUrl] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchToken() {
      try {
        const res = await fetch(
          `${API_BASE}/api/v1/metabase/token?dashboard_id=${dashboardId}&user_role=${userRole}`
        );
        const data = await res.json();

        if (data.status === "error") {
          setError(data.message);
        } else {
          setIframeUrl(data.iframe_url);
        }
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    }

    fetchToken();
  }, [dashboardId, userRole]);

  if (loading) {
    return <div style={styles.container}><div style={styles.spinner}>Loading...</div></div>;
  }

  if (error) {
    return <div style={styles.container}><div style={styles.error}>{error}</div></div>;
  }

  return (
    <div style={styles.container}>
      <iframe
        src={iframeUrl}
        frameBorder="0"
        width="100%"
        height="100%"
        style={styles.iframe}
        title={`Dashboard ${dashboardId}`}
        allowTransparency
      />
    </div>
  );
}

const styles = {
  container: {
    width: "100%",
    height: "600px",
    border: "1px solid #e0e0e0",
    borderRadius: "8px",
    overflow: "hidden",
    backgroundColor: "#fafafa",
  },
  iframe: {
    border: "none",
    width: "100%",
    height: "100%",
  },
  spinner: {
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    height: "100%",
    fontSize: "16px",
    color: "#666",
  },
  error: {
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    height: "100%",
    fontSize: "16px",
    color: "#d32f2f",
  },
};
