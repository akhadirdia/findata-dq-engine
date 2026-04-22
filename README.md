# findata-dq-engine

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi)
![Coverage](https://img.shields.io/badge/coverage-91%25-brightgreen)
![License](https://img.shields.io/badge/license-MIT-lightgrey)
![Status](https://img.shields.io/badge/status-in%20progress-orange)

**A financial data quality engine that detects, classifies, and remediates dirty records — before they corrupt your models, trigger regulatory fines, or pay fraudulent claims.**

---

## Demo

```bash
curl -X POST http://localhost:8001/validate \
  -H "Content-Type: application/json" \
  -d '{"records": [{"num_police": "AU-000001", "prime_annuelle": "-200", "date_expiration": "2023-01-01", "date_effet": "2024-06-01"}], "dataset": "policies"}'
```

```json
{
  "global_dq_score": 41.7,
  "nb_iv_total": 3,
  "nb_iv_high_impact": 2,
  "nb_records_mastered_eligible": 0,
  "pipeline_duration_seconds": 0.003,
  "by_dimension": {
    "BusinessRules": { "dimension_score": 0.0, "nb_iv": 2 },
    "Timeliness":    { "dimension_score": 0.0, "nb_iv": 1 }
  }
}
```

> Interactive Swagger UI: `http://localhost:8001/docs` · Streamlit dashboard: `http://localhost:8501`

---

## Problem Statement

Financial and insurance data pipelines fail silently. A policy with a negative premium, an expiration date before its effective date, or a missing mandatory field passes through ETL layers undetected — until it causes an underwriting error, a fraudulent payout, or a Solvency II audit finding.

Generic data quality tools apply statistical rules. They have no knowledge of insurance business logic: what constitutes a valid coverage type, when a claim amount becomes suspicious relative to the insured sum, or which fields are legally mandatory per regulatory framework.

This engine applies 12 domain-specific quality dimensions — structured as a Raw → Staged → Mastered pipeline — so invalid records are blocked before they reach production.

---

## Architecture

```
CSV / API / JSON
       │
       ▼
┌──────────────────────────────────────────────┐
│              ORCHESTRATOR                    │
│  ┌─────────────────────────────────────────┐ │
│  │         12 DQ DIMENSIONS                │ │
│  │                                         │ │
│  │  Completeness  Conformity   Coherence   │ │
│  │  Timeliness    Congruence   Privacy     │ │
│  │  BusinessRules Precision    Accuracy    │ │
│  │  Collection    Fairness     ModelDrift  │ │
│  └──────────────┬──────────────────────────┘ │
│                 │  per-record: V / S / IV     │
└─────────────────┼────────────────────────────┘
                  │
       ┌──────────┴────────────┐
       ▼                       ▼
  STAGED results         ISOLATION FOREST
  (rule-based)           (anomaly / fraud ML)
       │                       │
       └──────────┬────────────┘
                  ▼
          LLM REMEDIATION
       (Claude Haiku — action +
        confidence + business rationale)
                  │
       ┌──────────┴────────────┐
       ▼                       ▼
  SCORECARD JSON          MASTERED set
  + FastAPI /validate     (IV-critical blocked)
       │
       ▼
  Streamlit dashboard
  (heatmap · drill-down · anomaly view)
```

**Record statuses:** `V` (Valid) · `S` (Suspect — passes with warning) · `IV` (Invalid — blocked from Mastered)

---

## Key Technical Decisions

### 1. Buzzelli Extended as the dimension framework — not a custom taxonomy

Most DQ tools define dimensions ad hoc. Buzzelli Extended provides a peer-reviewed, finance-specific framework covering completeness, timeliness, business rules, fairness, and model drift. Using an established taxonomy makes the scoring defensible in audit contexts and forces completeness — dimensions you wouldn't think to add (Collection, ModelDrift) turn out to be the ones that catch subtle data poisoning.

### 2. Isolation Forest for anomaly detection — not rule thresholds

Rule-based fraud detection requires domain experts to define every threshold manually. Isolation Forest learns the normal distribution of the dataset and flags statistical outliers without labelled training data. This matters in insurance, where fraud patterns shift. The tradeoff: it requires enough records to build a meaningful model (minimum ~50), so the engine degrades gracefully to rule-only mode when `ml_enabled=False`.

### 3. LLM remediation as a last pass — not a first filter

LLMs are called only after rule-based dimensions have already classified a record as `IV`. This keeps costs predictable (Claude Haiku, not Sonnet), avoids hallucinated corrections on valid data, and ensures every LLM suggestion is grounded in a specific, already-identified defect. The response includes a `confidence` score so the calling system can decide whether to auto-fix or route to human review.

### 4. FastAPI over Flask — strict schema validation at the boundary

Pydantic v2 models on both request and response mean invalid payloads are rejected with structured 422 errors before they enter the pipeline. This makes the API self-documenting (Swagger) and eliminates an entire class of defensive checks inside the engine. Flask would have required explicit validation middleware.

### 5. Raw → Staged → Mastered as the data contract — not a pass/fail boolean

A single pass/fail on a record destroys information. The three-tier contract preserves the full picture: `S` records reach Staged so analysts can review them; only `IV`-critical records are hard-blocked from Mastered. Downstream teams get a score (`global_dq_score`), not just a green/red signal, so they can set their own acceptance thresholds.

---

## Features

- **12 domain-specific dimensions** — Buzzelli Extended framework, finance and insurance context
- **ML anomaly detection** — Isolation Forest with ≥85% fraud recall on simulated claims data
- **LLM remediation** — Claude Haiku suggests `auto_fix`, `human_review`, or `reject` with confidence score
- **REST API** — single `POST /validate` endpoint, Pydantic v2 schemas, interactive Swagger UI
- **Streamlit dashboard** — heatmap by status × dimension, drill-down on IV records, anomaly overlay

---

## Tech Stack

| Component | Technology | Reason |
|-----------|------------|--------|
| Core engine | Python 3.12 | Pattern matching, typed dicts — cleaner dimension logic |
| Data validation | Pydantic v2 | 10× faster than v1; strict mode catches type coercions |
| API layer | FastAPI | Auto-generates OpenAPI spec; async-ready for future scale |
| ML detection | Scikit-learn IsolationForest | No labelled data required; lightweight enough for per-batch inference |
| LLM remediation | Anthropic Claude Haiku | Lowest latency/cost in the Claude family; sufficient for structured JSON output |
| Dashboard | Streamlit + Plotly | Rapid prototyping; Plotly for interactive heatmaps without a JS build step |
| Containerization | Docker multi-stage | Builder + runtime stages keep the image under 200 MB |
| CI | GitHub Actions | Native Python + Docker caching; ruff + pytest --cov in a single job |

---

## Quick Start

```bash
# 1. Clone and install
git clone https://github.com/akhadirdia/findata-dq-engine.git
cd findata-dq-engine
pip install -r requirements.txt

# 2. Start the API
uvicorn api.main:app --reload --port 8001

# 3. Validate records (Swagger UI at http://localhost:8001/docs)
curl -X POST http://localhost:8001/validate \
  -H "Content-Type: application/json" \
  -d @tests/fixtures/sample_policies.json

# 4. (Optional) Enable LLM remediation
cp .env.example .env  # add ANTHROPIC_API_KEY

# 5. (Optional) Launch dashboard
streamlit run dashboard/app.py
```

Or with Docker:

```bash
docker compose up
# API → http://localhost:8000  |  Dashboard → http://localhost:8501
```

---

## Project Structure

```
findata-dq-engine/
├── findata_dq/
│   ├── dimensions/          # One file per DQ dimension (12 total)
│   │   ├── base.py          # Abstract DQDimension class
│   │   ├── completeness.py
│   │   ├── business_rules.py
│   │   ├── fairness.py
│   │   └── ...              # 9 other dimensions
│   ├── pipeline/
│   │   └── orchestrator.py  # Runs all dimensions, aggregates scorecard
│   ├── ai/                  # Isolation Forest + LLM remediation
│   ├── models/              # Pydantic models for internal data (DQResult, Scorecard)
│   └── utils/               # Shared helpers (date parsing, type coercion)
├── api/
│   ├── main.py              # FastAPI app, CORS, router mount
│   ├── routers/validate.py  # POST /validate
│   └── schemas/validate.py  # Request / response Pydantic models
├── dashboard/
│   └── app.py               # Streamlit UI — heatmap, drill-down, anomaly view
├── tests/
│   ├── unit/                # 208 tests — dimensions, API, ML, LLM
│   └── fixtures/            # Synthetic policy, claim, and log datasets
├── Dockerfile               # Multi-stage build (builder + runtime)
├── docker-compose.yml       # API + dashboard services with health checks
├── pyproject.toml           # Dependencies, ruff, mypy, pytest config
└── .github/workflows/ci.yml # Lint → test → docker build
```

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | No | — | Enables LLM remediation (Claude Haiku). Without it, `llm_enabled` is ignored |
| `PIPELINE_ENV` | No | `production` | `production` / `staging` — affects scoring thresholds |
| `LOG_LEVEL` | No | `INFO` | `DEBUG` for per-dimension trace output |

---

## Roadmap

1. **Streaming validation** — replace the batch `POST /validate` with a WebSocket endpoint so large datasets (1M+ records) stream results without holding a single HTTP connection open
2. **Dataset adapters** — currently policies, claims, logs, and ML models are hard-coded. A pluggable adapter pattern would let users register custom schemas without touching dimension logic
3. **Drift monitoring endpoint** — expose `GET /drift?dataset=policies&window=30d` to track `global_dq_score` over time and surface degradation before it hits production models

---

## Author

**Abdou Khadir DIA** — Senior Data Scientist · ML · Generative AI

[LinkedIn](https://linkedin.com/in/abdou-khadir-dia-284012130) · [GitHub](https://github.com/akhadirdia)
