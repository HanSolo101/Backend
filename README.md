# Glynac Backend Ingestion Platform

Enterprise-grade backend ingestion services for Salesforce, HubSpot, and Slack data pipelines with MinIO object storage, ClickHouse analytics, and a visual monitoring dashboard.

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                  GLYNAC INGESTION PLATFORM                     │
│                                                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│  │  Salesforce   │  │   HubSpot    │  │    Slack     │        │
│  │  Bulk API v2  │  │ dlt Pipeline │  │  Dual-Mode   │        │
│  │  12 Objects   │  │  8 Resources │  │  Backfill +  │        │
│  │              │  │  Parquet     │  │  Real-time   │        │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘        │
│         │                 │                  │                 │
│         └─────────────────┼──────────────────┘                │
│                           ▼                                    │
│              ┌──────────────────────┐                          │
│              │  MinIO + ClickHouse  │                          │
│              └──────────────────────┘                          │
│                           ▼                                    │
│              ┌──────────────────────┐                          │
│              │   Monitoring Web UI  │                          │
│              └──────────────────────┘                          │
└────────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Start Infrastructure (Docker)

```bash
docker-compose up -d
```

This starts:
- **MinIO** on `localhost:9000` (console: `localhost:9001`)
- **ClickHouse** on `localhost:8123`
- Auto-creates buckets: `salesforce`, `hubspot`, `slack`, `checkpoints`

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the Platform

```bash
python main.py
```

This starts:
- **Mock API servers** (Salesforce `:8100`, HubSpot `:8200`, Slack `:8300`)
- **Main API server** on `localhost:8000`
- **Web Monitoring Dashboard** at `http://localhost:8000`

### 4. Use the Dashboard

Open `http://localhost:8000` in your browser to access the monitoring console.

## Task 1: Salesforce Bulk API Ingestion

### Objects Extracted (12)
`Account` · `Contact` · `Opportunity` · `Lead` · `Task` · `Case` · `Product2` · `PricebookEntry` · `Contract` · `Asset` · `Campaign` · `Event`

### API Endpoints
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/salesforce/sync` | Trigger bulk sync for all objects |
| POST | `/api/salesforce/sync/single` | Sync a single object |
| GET | `/api/salesforce/status` | Get pipeline status |
| POST | `/api/salesforce/pause` | Pause pipeline |
| POST | `/api/salesforce/resume` | Resume pipeline |
| POST | `/api/salesforce/reset` | Reset checkpoints |
| GET | `/api/salesforce/clickhouse/tables` | List ClickHouse tables |
| GET | `/api/salesforce/clickhouse/query?table=...&limit=100` | Query a table |
| GET | `/api/salesforce/minio/files` | Browse MinIO files |

### Features
- OAuth2 authentication with Salesforce Bulk API v2
- Exponential backoff retry for rate limits (HTTP 429)
- Parallel ingestion across objects (configurable workers)
- Parquet files landed in MinIO under `/salesforce/{object}/{org_id}/{date}/`
- Auto-schema ClickHouse table creation for all 12 objects
- Analytical views (`v_salesforce_pipeline_summary`, `v_salesforce_account_overview`, etc.)

## Task 2: HubSpot dlt Pipeline

### Resources Extracted (8)
`contacts` · `companies` · `deals` · `tickets` · `line_items` · `engagements` · `pipelines` · `owners`

### API Endpoints
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/hubspot/run` | Run the dlt pipeline |
| POST | `/api/hubspot/run/single` | Run for a single resource |
| GET | `/api/hubspot/status` | Get pipeline status |
| POST | `/api/hubspot/pause` | Pause pipeline |
| POST | `/api/hubspot/resume` | Resume pipeline |
| GET | `/api/hubspot/clickhouse/views` | List analytical views |
| GET | `/api/hubspot/clickhouse/query?table=...` | Query a table/view |

### Features
- dlt-style pipeline with Parquet columnar output
- MinIO landing under `/hubspot/{resource}/year=YYYY/month=MM/`
- Parallel resource extraction with thread pool
- Cursor-based state checkpointing for incremental loads
- Pause/Resume with persistent state in MinIO
- Crash recovery — resumes from last checkpoint
- 5 analytical ClickHouse views (`v_hubspot_deal_pipeline`, `v_hubspot_contact_lifecycle`, etc.)

## Task 3: Slack Dual-Mode Ingestion

### Dual Modes
1. **Historical Backfill** — Fetches all workspace history (channels, users, messages, threads, reactions)
2. **Real-time Streaming** — WebSocket connection for live events

### API Endpoints
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/slack/backfill/start` | Start historical backfill |
| POST | `/api/slack/backfill/pause` | Pause backfill |
| POST | `/api/slack/backfill/resume` | Resume backfill |
| POST | `/api/slack/realtime/start` | Start real-time stream |
| POST | `/api/slack/realtime/stop` | Stop real-time stream |
| POST | `/api/slack/realtime/pause` | Pause stream |
| GET | `/api/slack/status` | Combined status |
| GET | `/api/slack/clickhouse/query?table=...` | Query tables/views |

### Features
- Parallel channel backfill with per-channel checkpointing
- WebSocket real-time event streaming with <1s latency
- Buffered Parquet flushes (50 events or 10 seconds)
- High-water mark tracking per channel
- Users and channels as reference Parquet files
- 4 analytical views (`v_slack_compliance_timeline`, `v_slack_channel_activity`, etc.)
- Crash recovery with zero data duplication

## Project Structure

```
Backend/
├── docker-compose.yml          # MinIO + ClickHouse
├── requirements.txt            # Python dependencies
├── main.py                     # Unified API entry point
├── .env                        # Configuration
├── shared/                     # Shared utilities
│   ├── config.py              # Central settings
│   ├── minio_client.py        # MinIO operations
│   ├── clickhouse_client.py   # ClickHouse operations
│   ├── parquet_writer.py      # PyArrow Parquet conversion
│   └── checkpoint.py          # Pause/Resume/Crash recovery
├── mock_servers/               # Synthetic API simulators
│   ├── salesforce_mock.py     # Bulk API v2 (12 objects)
│   ├── hubspot_mock.py        # CRM API (8 resources)
│   └── slack_mock.py          # Web API + WebSocket
├── task1_salesforce/           # Salesforce Bulk API Service
│   ├── bulk_api_client.py     # OAuth2 + Bulk API v2 client
│   ├── ingestion_worker.py    # Parallel ingestion orchestrator
│   ├── clickhouse_loader.py   # Schema + table management
│   └── api.py                 # FastAPI router
├── task2_hubspot/              # HubSpot dlt Pipeline
│   ├── hubspot_pipeline.py    # Extract + Parquet + MinIO
│   ├── clickhouse_views.py    # Tables + analytical views
│   └── api.py                 # FastAPI router
├── task3_slack/                # Slack Dual-Mode Engine
│   ├── historical_backfill.py # Mode 1: Historical
│   ├── realtime_stream.py     # Mode 2: Real-time WebSocket
│   ├── clickhouse_loader.py   # Tables + compliance views
│   └── api.py                 # FastAPI router
└── web_ui/
    └── index.html             # Monitoring dashboard
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Mock API servers** | Enable full development without production credentials |
| **Shared checkpoint module** | Consistent pause/resume/crash-recovery across all 3 tasks |
| **Parquet via PyArrow** | Efficient columnar compression, ClickHouse-optimized |
| **ThreadPoolExecutor** | Parallel object/resource/channel ingestion |
| **MinIO checkpoint storage** | Survives process crashes (not in-memory) |
| **Tenacity retry** | Exponential backoff for API rate limits |

## Credentials

| Service | User | Password |
|---------|------|----------|
| MinIO | `minioadmin` | `minioadmin123` |
| MinIO Console | `minioadmin` | `minioadmin123` |
| ClickHouse | `default` | `clickhouse123` |
