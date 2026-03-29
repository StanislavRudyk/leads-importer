# Lead Intelligence Hub

A production-grade, high-performance ecosystem for sophisticated lead ingestion, geographic enrichment, and real-time analytics. This platform transforms fragmented data sources into a centralized, intelligence-driven database with zero-manual-effort categorization.

---

## 📈 Project Evolution & Implementation Log

Over the course of the project development, the system has undergone a major transformation. Below is a detailed log of the successfully implemented features and logic refinements:

### 🌍 Automated Geographic Intelligence
- **Implemented**: Path-based location inference. The system now automatically detects city and country origins by recursively analyzing the directory structure (e.g., `Mailchimp/(1) USA/Chicago/`).
- **Improved**: Robust parsing logic that handles inconsistent folder depths, case-insensitivity, and filters out system noise (like folder indexes or master list markers).

### ⚡ Database & Ingestion Optimization
- **Implemented**: High-performance `UPSERT` engine. Replaced slow row-by-row saving with bulk `ON CONFLICT (email) DO UPDATE` operations.
- **Implemented**: Intelligent batch processing. The system now processes data in chunked groups (200-500 rows) to prevent memory overhead and handle 100k+ row datasets seamlessly.
- **Changed**: Fixed dashboard SQL queries. Removed restrictive city filters, ensuring that 100% of leads—including those awaiting categorization—are visible on the overview.

### 🔐 Security & Architecture Hardening
- **Implemented**: Full environment isolation. Removed all hardcoded internal keys and credentials from `config.py` and `docker-compose.yml`.
- **Implemented**: Dedicated `.env` security. Sensitive data (API keys, DB URLs) is now managed through encrypted or local-only environment variables.
- **Changed**: Code sanitization. All developer comments and internal notes (`#`) have been stripped from the source code for a minimalist, "clean-code" professional look.

### ⚙️ Mass Import & Orchestration
- **Implemented**: Recursive mass-import script. Created `run_mass_import.py` to handle 480+ files in a single run with granular source naming based on filenames.
- **Improved**: Ingestion prioritization. Files are now sorted by size, allowing for immediate feedback on the dashboard as the system works through the queue.

---

## 🧠 System Mechanics

### 1. Unified Merging & Deduplication
The system uses the **Email Address** as the primary unique identifier.
- **Field Merging**: On conflict, the system performs a non-destructive merge. Empty fields are filled with new data, and existing fields are updated according to source-level priorities.
- **Tag Aggregation**: Tags from multiple imports are combined into a unique set using PostgreSQL JSONB set operations.
- **Audit Logging**: Every lead preserves its full import history (filenames and timestamps) in the `meta_info` JSONB column.

### 2. Geo-Parsing Logic
The `parser.py` engine identifies the 'Home' segment of the file path.
- It identifies the **Country** segment (e.g., USA, Europe, Asia).
- It identifies the **City** folder as the segment immediately following the country segment, provided it is not a system or filename part.
- This data is then normalized via `normalizer.py` to ISO-3166 codes and standard city titles.

---

## 🚀 Deployment & Operations

### Deployment directly with Docker
The entire ecosystem is containerized for zero-dependency deployment.
```bash
cp .env.example .env
docker-compose up -d --build
```

### Running the Full Archive Import
To trigger the induction of the entire `Mailchimp/` data library:
```bash
docker exec leads_backend python -m src.run_mass_import
```

---

## 🛠 Tech Stack

- **Core**: Python 3.10+, FastAPI (Async), SQLAlchemy 2.0.
- **Data Engine**: Pandas (Stream-reading), PostgreSQL 15 (JSONB optimized).
- **Frontend**: React 18, Vite, Metabase Embedding.
- **Infrastructure**: Docker, Nginx (Reverse Proxy), n8n (Integrations).

---
*Designed for intelligence. Professional production release v1.0.0.*
