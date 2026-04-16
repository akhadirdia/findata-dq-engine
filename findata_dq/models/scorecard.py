"""
Modèles pour la scorecard DQ et les rapports finaux.
C'est l'output principal du pipeline après passage par les 12 dimensions.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, computed_field

from findata_dq.models.dq_result import DQResult, RecordSummary


# ─── Résumé par dimension ─────────────────────────────────────────────────────

class DimensionSummary(BaseModel):
    """Agrégat des résultats pour une dimension donnée."""

    dimension: str
    nb_tested: int = 0
    nb_v: int = 0
    nb_s: int = 0
    nb_iv: int = 0
    nb_iv_high: int = 0   # IV avec Impact H
    dimension_score: float = Field(ge=0.0, le=1.0, default=1.0)

    @computed_field
    @property
    def pass_rate(self) -> float:
        if self.nb_tested == 0:
            return 1.0
        return round(self.nb_v / self.nb_tested, 4)


# ─── Impact financier ─────────────────────────────────────────────────────────

class FinancialImpact(BaseModel):
    """Estimation de l'impact financier des données IV non corrigées."""

    total_usd: float = 0.0
    by_dimension: dict[str, float] = Field(default_factory=dict)
    by_impact_level: dict[str, float] = Field(
        default_factory=lambda: {"H": 0.0, "M": 0.0, "L": 0.0}
    )
    methodology: str = "Estimation basée sur les règles métier et barèmes configurés"
    currency: str = "CAD"


# ─── Scorecard principale ────────────────────────────────────────────────────

class Scorecard(BaseModel):
    """
    Output principal du pipeline DQ.
    Contient tous les DQResult + agrégats pour affichage dans le dashboard.
    """

    scorecard_id: str = Field(description="UUID unique de cette exécution du pipeline")
    dataset: str = Field(description="Nom du dataset analysé")
    pipeline_env: Literal["development", "staging", "production"] = "development"
    evaluated_at: datetime = Field(default_factory=datetime.utcnow)

    # Données brutes
    total_records: int = 0
    total_fields_tested: int = 0
    results: list[DQResult] = Field(default_factory=list)

    # Agrégats par dimension (pour la heatmap)
    by_dimension: dict[str, DimensionSummary] = Field(default_factory=dict)

    # Agrégats par enregistrement (pour le drill-down)
    by_record: dict[str, RecordSummary] = Field(default_factory=dict)

    # Score global
    global_dq_score: float = Field(
        ge=0.0, le=100.0, default=0.0,
        description="Score global DQ sur 100 — moyenne pondérée des 12 dimensions"
    )
    nb_iv_total: int = 0
    nb_iv_high_impact: int = 0
    nb_s_total: int = 0
    nb_records_mastered_eligible: int = 0

    # Impact financier
    financial_impact: FinancialImpact = Field(default_factory=FinancialImpact)

    # Anomalies ML (Isolation Forest)
    nb_ml_anomalies: int = 0
    ml_anomaly_record_ids: list[str] = Field(default_factory=list)

    # Coût LLM (remédiation)
    llm_cost_usd: float = 0.0
    nb_llm_remediations: int = 0

    # Timing
    pipeline_duration_seconds: Optional[float] = None

    @computed_field
    @property
    def pass_rate(self) -> float:
        if self.total_fields_tested == 0:
            return 1.0
        nb_v = sum(1 for r in self.results if r.status == "V")
        return round(nb_v / self.total_fields_tested, 4)

    def get_iv_results(self, impact: Optional[str] = None) -> list[DQResult]:
        """Retourne les résultats IV, filtrés par niveau d'impact si fourni."""
        ivs = [r for r in self.results if r.status == "IV"]
        if impact:
            ivs = [r for r in ivs if r.impact == impact]
        return ivs

    def get_results_by_dimension(self, dimension: str) -> list[DQResult]:
        return [r for r in self.results if r.dimension == dimension]

    def to_heatmap_data(self) -> list[dict[str, Any]]:
        """
        Retourne les données formatées pour la heatmap Plotly.
        Format : liste de {record_id, dimension, status, impact, score}
        """
        return [
            {
                "record_id": r.record_id,
                "field": r.field_name,
                "dimension": r.dimension,
                "status": r.status,
                "impact": r.impact,
                "score": r.score,
            }
            for r in self.results
        ]


# ─── Rapport final ────────────────────────────────────────────────────────────

class DQReport(BaseModel):
    """
    Rapport exécutif complet, prêt pour le Comité des Risques.
    Généré automatiquement par le LLM (v2) ou synthétisé par règles (v1).
    """

    report_id: str
    scorecard_id: str = Field(description="Référence vers Scorecard.scorecard_id")
    dataset: str
    period: str = Field(description="Période analysée ex: 2026-04-14")
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    generated_by: Literal["LLM", "rule_based"] = "rule_based"

    # Contenu exécutif
    situation_globale: str = ""
    risques_critiques: list[str] = Field(default_factory=list)
    actions_prioritaires: list[str] = Field(default_factory=list)
    recommandations: list[str] = Field(default_factory=list)

    # Métriques clés
    global_dq_score: float
    nb_iv_high_impact: int
    financial_impact_cad: float
    trend_vs_previous: Optional[str] = Field(
        default=None,
        description="Comparaison avec la scorecard précédente ex: +2.3 points"
    )

    # Conformité réglementaire
    loi25_status: Optional[Literal["conforme", "non_conforme", "en_cours"]] = None
    ai_act_flags: list[str] = Field(default_factory=list)
