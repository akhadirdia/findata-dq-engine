"""
findata-dq-engine — API FastAPI
Lancer : uvicorn api.main:app --reload

Endpoints
---------
GET  /          → info
GET  /health    → health check
POST /validate  → pipeline DQ complet (12 dimensions)
GET  /docs      → Swagger UI
GET  /redoc     → ReDoc
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import validate as validate_router

# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="findata-dq-engine API",
    description=(
        "Moteur de qualité des données financières et d'assurance. "
        "Framework Buzzelli Extended — 12 dimensions, Isolation Forest, remédiation LLM."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── CORS (dev : tout autoriser) ───────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────

app.include_router(validate_router.router)


# ── Routes utilitaires ────────────────────────────────────────────────────────

@app.get("/", tags=["Info"], summary="Informations sur l'API")
def root() -> dict:
    return {
        "service": "findata-dq-engine",
        "version": "1.0.0",
        "framework": "Buzzelli Extended — 12 dimensions",
        "docs": "/docs",
        "endpoints": {
            "POST /validate": "Lancer le pipeline DQ sur un ensemble d'enregistrements",
            "GET  /health":   "Vérifier que l'API est opérationnelle",
        },
    }


@app.get("/health", tags=["Info"], summary="Health check")
def health() -> dict:
    return {"status": "ok", "service": "findata-dq-engine"}
