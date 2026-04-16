"""
Schémas Pydantic pour l'API FastAPI.
Séparés des modèles internes pour découpler la couche API du moteur DQ.
"""

from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


# ── Requête ───────────────────────────────────────────────────────────────────

class ValidateRequest(BaseModel):
    """
    Corps de la requête POST /validate.

    Exemple minimal :
    {
        "records": [{"num_police": "AU-001", "prime_annuelle": "1500", ...}],
        "dataset": "policies"
    }
    """

    records: list[dict[str, Any]] = Field(
        description="Liste des enregistrements à valider (dicts clé/valeur).",
        min_length=1,
    )
    dataset: Literal["policies", "claims", "logs", "model_metadata"] = Field(
        default="policies",
        description="Type de dataset — détermine les dimensions et règles métier appliquées.",
    )
    pipeline_env: Literal["development", "staging", "production"] = Field(
        default="development",
        description="Environnement d'exécution (influence les seuils de tolérance).",
    )
    ml_enabled: bool = Field(
        default=True,
        description="Activer la détection d'anomalies Isolation Forest.",
    )
    llm_enabled: bool = Field(
        default=False,
        description="Activer la remédiation LLM via Claude API (génère des coûts API).",
    )
    include_raw_results: bool = Field(
        default=False,
        description=(
            "Inclure la liste complète des DQResult dans la réponse. "
            "Peut être volumineux sur de grands datasets — désactivé par défaut."
        ),
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "records": [
                    {
                        "num_police": "AU-000001",
                        "id_client": "CLI-0001",
                        "date_effet": "2024-06-01",
                        "date_expiration": "2025-06-01",
                        "type_couverture": "auto",
                        "prime_annuelle": "1500.00",
                        "montant_assure": "50000.00",
                        "statut_police": "active",
                        "franchise": "500",
                        "date_creation": "2024-05-25",
                    }
                ],
                "dataset": "policies",
                "pipeline_env": "development",
                "ml_enabled": False,
                "llm_enabled": False,
                "include_raw_results": False,
            }
        }
    }


# ── Réponse résumée par dimension ─────────────────────────────────────────────

class DimensionSummaryOut(BaseModel):
    dimension: str
    nb_tested: int
    nb_v: int
    nb_s: int
    nb_iv: int
    nb_iv_high: int
    dimension_score: float
    pass_rate: float


# ── Réponse principale ────────────────────────────────────────────────────────

class ValidateResponse(BaseModel):
    """
    Réponse de POST /validate.
    Contient le résumé de la scorecard DQ + détails optionnels.
    """

    scorecard_id: str
    dataset: str
    pipeline_env: str
    evaluated_at: str           # ISO 8601

    # KPIs globaux
    total_records: int
    total_fields_tested: int
    global_dq_score: float      # /100
    pass_rate: float            # 0–1
    nb_iv_total: int
    nb_iv_high_impact: int
    nb_s_total: int
    nb_records_mastered_eligible: int
    pipeline_duration_seconds: Optional[float]

    # ML
    nb_ml_anomalies: int
    ml_anomaly_record_ids: list[str]

    # LLM
    llm_cost_usd: float
    nb_llm_remediations: int

    # Détail par dimension
    by_dimension: dict[str, DimensionSummaryOut]

    # Résultats bruts (optionnels — activés via include_raw_results)
    raw_results: Optional[list[dict[str, Any]]] = None
