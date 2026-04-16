"""
Modèles de base du pipeline DQ.
DQResult est le type de retour universel de toutes les dimensions.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

# ─── Constantes de classification ─────────────────────────────────────────────

class DQStatus:
    VALID   = "V"   # Vert — dans la tolérance
    SUSPECT = "S"   # Jaune — à surveiller
    INVALID = "IV"  # Rouge — hors tolérance, bloqué


class ImpactLevel:
    HIGH   = "H"   # Impact critique (amendes, perte financière directe)
    MEDIUM = "M"   # Processus ralenti, correction requise sous 24h
    LOW    = "L"   # Anomalie mineure, système fonctionnel


# ─── Remédiation LLM ──────────────────────────────────────────────────────────

class RemediationResult(BaseModel):
    """Suggestion de correction générée par le LLM ou une règle déterministe."""

    suggested_value: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    action: Literal["auto_fix", "human_review", "reject"]
    explanation: str
    impact_si_non_corrige: str
    generated_by: Literal["LLM", "rule_based"] = "LLM"
    generated_at: datetime = Field(default_factory=datetime.utcnow)


# ─── Résultat DQ principal ────────────────────────────────────────────────────

class DQResult(BaseModel):
    """
    Résultat de validation pour un champ donné d'un enregistrement.
    Retourné par toutes les dimensions (BaseDimension.validate → list[DQResult]).
    """

    # Identification
    datum_id: str = Field(description="Identifiant unique du résultat : {record_id}_{field_name}_{dimension}")
    dataset: str = Field(description="Nom du dataset source : policies, claims, logs, model_metadata")
    record_id: str = Field(description="Identifiant de la ligne testée")
    field_name: str = Field(description="Nom de la colonne testée")
    field_value: str | None = Field(default=None, description="Valeur testée (masquée si PII)")

    # Classification DQ
    dimension: str = Field(description="Nom de la dimension Buzzelli appliquée")
    status: Literal["V", "S", "IV"]
    impact: Literal["H", "M", "L"]
    score: float = Field(ge=0.0, le=1.0, description="Score DQ normalisé : 1.0=V, 0.5=S, 0.0=IV")

    # Contexte de la règle
    rule_applied: str = Field(description="Description lisible de la règle évaluée")
    details: dict[str, Any] = Field(default_factory=dict, description="Valeurs intermédiaires de calcul")

    # Impact financier
    financial_impact_usd: float | None = Field(
        default=None,
        description="Estimation de l'impact financier en USD si la donnée IV n'est pas corrigée"
    )

    # Remédiation (rempli après passage par la couche LLM)
    remediation: RemediationResult | None = None

    # Traçabilité
    evaluated_at: datetime = Field(default_factory=datetime.utcnow)
    pipeline_env: str | None = None

    @model_validator(mode="after")
    def set_score_from_status(self) -> DQResult:
        """Normalise le score DQ selon le statut si non fourni explicitement."""
        if self.score == 0.0 and self.status != "IV":
            score_map = {"V": 1.0, "S": 0.5, "IV": 0.0}
            object.__setattr__(self, "score", score_map[self.status])
        return self

    @property
    def is_blocking(self) -> bool:
        """Vrai si la donnée doit être bloquée avant d'atteindre l'état Mastered."""
        return self.status == "IV" and self.impact == "H"

    def __repr__(self) -> str:
        return (
            f"DQResult(dataset={self.dataset!r}, record={self.record_id!r}, "
            f"field={self.field_name!r}, dim={self.dimension!r}, "
            f"status={self.status!r}, impact={self.impact!r})"
        )


# ─── Résumé agrégé par enregistrement ────────────────────────────────────────

class RecordSummary(BaseModel):
    """Agrégat DQ pour un enregistrement complet (toutes dimensions confondues)."""

    record_id: str
    dataset: str
    worst_status: Literal["V", "S", "IV"]
    dimensions_iv: list[str] = Field(default_factory=list)
    dimensions_s: list[str] = Field(default_factory=list)
    global_score: float = Field(ge=0.0, le=1.0)
    is_mastered_eligible: bool = Field(
        description="Vrai si aucun IV avec Impact H — éligible à passer en Mastered"
    )
    financial_impact_total_usd: float = 0.0
