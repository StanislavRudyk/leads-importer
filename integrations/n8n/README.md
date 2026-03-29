# n8n Integration for Leads Importer

This directory contains everything you need to connect your leads ingestion pipeline to **n8n.io**.

##  Files Included
- **`leads_importer_workflow.json`**: A preconfigured n8n workflow.

## How to Setup

### 1. Import Workflow
1. Open your n8n instance.
2. Click **Workflows** > **Import from File**.
3. Select `leads_importer_workflow.json`.

### 2. Configure Credentials
1. In the **HTTP Request** node, you'll need to add your API Key.
2. Go to **Credentials** > **New Credentials** > **Header Auth**.
3. Use the following:
   - **Name**: `API-Key`
   - **Value**: `gmp79b9qSN}&JWX` (or your actual API key from `.env`).

### 3. Connect Notifications
1. Open the **Slack Notification** (or Telegram) node.
2. Connect your Slack App token or Webhook.
3. The message is already pre-mapped to show:
   - Total rows processed.
   - Inserted/Updated/Skipped counts.

##  Endpoint Details
- **Base URL**: `http://leads-importer:8000` (or your server IP).
- **Import Path**: `/api/v1/import/upload`.
- **Method**: `POST`.
- **Content-Type**: `multipart/form-data`.
