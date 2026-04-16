"""
Router POST /validate — point d'entrée principal du moteur DQ.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Assure que findata_dq est importable même en lançant uvicorn depuis api/
_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from fastapi import APIRouter, HTTPException, status

from findata_dq.pipeline.orchestrator import DQOrchestrator, OrchestratorConfig
from api.schemas.validate import (
    DimensionSummaryOut,
    ValidateRequest,
    ValidateResponse,
)

router = APIRouter(prefix="/validate", tags=["Validation DQ"])


@router.post(
    "",
    response_model=ValidateResponse,
    status_code=status.HTTP_200_OK,
    summary="Valider un ensemble d'enregistrements",
    description=(
        "Lance le pipeline DQ complet (12 dimensions Buzzelli Extended) "
        "sur les enregistrements fournis et retourne une scorecard agrégée."
    ),
)
def validate(body: ValidateRequest) -> ValidateResponse:
    """
    Pipeline Raw → Staged → Mastered.

    - **records** : liste de dicts représentant les enregistrements à valider
    - **dataset** : `policies` | `claims` | `logs` | `model_metadata`
    - **ml_enabled** : active l'Isolation Forest (recommandé sur ≥ 20 enregistrements)
    - **llm_enabled** : active la remédiation Claude API (génère des coûts)
    - **include_raw_results** : inclure la liste brute des DQResult dans la réponse
    """
    cfg = OrchestratorConfig(
        pipeline_env=body.pipeline_env,
        ml_enabled=body.ml_enabled,
        llm_enabled=body.llm_enabled,
    )
    orch = DQOrchestrator(cfg)

    try:
        sc = orch.run(body.records, body.dataset)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Erreur pipeline DQ : {exc}",
        ) from exc

    # Construire le résumé par dimension
    by_dim_out: dict[str, DimensionSummaryOut] = {
        name: DimensionSummaryOut(
            dimension=ds.dimension,
            nb_tested=ds.nb_tested,
            nb_v=ds.nb_v,
            nb_s=ds.nb_s,
            nb_iv=ds.nb_iv,
            nb_iv_high=ds.nb_iv_high,
            dimension_score=ds.dimension_score,
            pass_rate=ds.pass_rate,
        )
        for name, ds in sc.by_dimension.items()
    }

    # Résultats bruts (optionnel)
    raw: list[dict] | None = None
    if body.include_raw_results:
        raw = [r.model_dump(mode="json") for r in sc.results]

    return ValidateResponse(
        scorecard_id=sc.scorecard_id,
        dataset=sc.dataset,
        pipeline_env=sc.pipeline_env,
        evaluated_at=sc.evaluated_at.isoformat(),
        total_records=sc.total_records,
        total_fields_tested=sc.total_fields_tested,
        global_dq_score=sc.global_dq_score,
        pass_rate=sc.pass_rate,
        nb_iv_total=sc.nb_iv_total,
        nb_iv_high_impact=sc.nb_iv_high_impact,
        nb_s_total=sc.nb_s_total,
        nb_records_mastered_eligible=sc.nb_records_mastered_eligible,
        pipeline_duration_seconds=sc.pipeline_duration_seconds,
        nb_ml_anomalies=sc.nb_ml_anomalies,
        ml_anomaly_record_ids=sc.ml_anomaly_record_ids,
        llm_cost_usd=sc.llm_cost_usd,
        nb_llm_remediations=sc.nb_llm_remediations,
        by_dimension=by_dim_out,
        raw_results=raw,
    )
