"""
Modèles Pydantic pour la gouvernance des modèles IA.
Domaine E : métadonnées modèles, métriques de biais, drift, explicabilité.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

# ─── Métadonnées de modèle ────────────────────────────────────────────────────

class ModelMetadata(BaseModel):
    """
    Représente les métadonnées d'un modèle IA en production ou en évaluation.
    Dataset : model_metadata.csv
    Utilisé pour : Dimension ModelDrift, PrivacyCompliance, Fairness.
    """

    model_id: str = Field(description="Identifiant unique du modèle")
    model_name: str = Field(description="Nom lisible ex: scoring_risque_auto_v2")
    model_version: str = Field(description="Version sémantique ex: 2.1.3")
    training_date: date = Field(description="Date d'entraînement — clé pour Timeliness")
    deployment_date: date | None = None
    statut_production: Literal["en_dev", "staging", "production", "retire", "archive"]

    # Métriques de performance
    accuracy: float | None = Field(default=None, ge=0.0, le=1.0)
    precision: float | None = Field(default=None, ge=0.0, le=1.0)
    recall: float | None = Field(default=None, ge=0.0, le=1.0)
    f1_score: float | None = Field(default=None, ge=0.0, le=1.0)
    auc_roc: float | None = Field(default=None, ge=0.0, le=1.0)

    # Dérive (ModelDrift dimension)
    drift_score: float | None = Field(
        default=None,
        ge=0.0,
        description="PSI global — V<0.10, S<0.25, IV>=0.25"
    )
    drift_status: Literal["V", "S", "IV"] | None = None
    last_drift_check: date | None = None

    # Conformité réglementaire
    ai_act_compliance_flag: Literal["compliant", "non_compliant", "under_review"] = "under_review"
    risque_vie_privee: Literal["faible", "modere", "eleve", "critique"] = "modere"
    features_sensibles: list[str] = Field(
        default_factory=list,
        description="Features PII ou proxies (age, sexe, code_postal)"
    )

    # Explicabilité
    shap_top_features: list[str] = Field(
        default_factory=list,
        description="Top features par importance SHAP"
    )
    shap_computed_at: date | None = None

    # Traçabilité
    owner_equipe: str | None = None
    date_creation: datetime | None = None

    @property
    def needs_retraining(self) -> bool:
        return self.drift_status == "IV"

    @property
    def is_in_production(self) -> bool:
        return self.statut_production == "production"


# ─── Métriques de biais / Fairness ───────────────────────────────────────────

class FairnessMetrics(BaseModel):
    """
    Métriques d'équité calculées par la Dimension Fairness.
    Une instance par (modèle, attribut protégé).
    """

    metric_id: str
    model_id: str = Field(description="Référence vers ModelMetadata.model_id")
    protected_attribute: str = Field(description="Attribut testé : sexe, age_groupe, code_postal")
    group_reference: str = Field(description="Groupe de référence ex: H (hommes)")
    group_compare: str = Field(description="Groupe comparé ex: F (femmes)")
    evaluation_date: date = Field(default_factory=date.today)

    # Métriques (voir Section 4 — Dimension 10)
    disparate_impact: float | None = Field(
        default=None,
        description="DI = P(défavorable|groupe_A) / P(défavorable|groupe_B). Seuil légal [0.80, 1.25]"
    )
    demographic_parity: float | None = Field(
        default=None,
        description="|P(score_haut|A) - P(score_haut|B)|. V<0.05, IV>=0.10"
    )
    equalized_odds: float | None = Field(
        default=None,
        description="|TPR_A - TPR_B|. V<0.05, IV>=0.10"
    )

    # Classification DQ
    status: Literal["V", "S", "IV"] = "V"
    violation_details: str | None = None
    mitigation_applied: str | None = None
    ai_act_article: str | None = Field(
        default=None,
        description="Article AI Act applicable ex: Article 10(3)"
    )


# ─── Valeurs SHAP ─────────────────────────────────────────────────────────────

class ShapResult(BaseModel):
    """
    Valeurs SHAP pour l'explicabilité d'une prédiction individuelle.
    Utilisé par la Dimension Fairness v2.
    """

    shap_id: str
    model_id: str
    record_id: str = Field(description="Enregistrement expliqué")
    prediction: float = Field(description="Valeur prédite par le modèle")
    base_value: float = Field(description="Valeur de base SHAP (E[f(x)])")
    feature_contributions: dict[str, float] = Field(
        description="Dict {feature_name: shap_value}"
    )
    top_features: list[str] = Field(
        default_factory=list,
        description="Top 5 features par contribution absolue"
    )
    computed_at: datetime = Field(default_factory=datetime.utcnow)


# ─── Audit trail modèle ───────────────────────────────────────────────────────

class ModelAuditLog(BaseModel):
    """
    Trace chaque décision prise par un modèle en production.
    Requis pour la conformité AI Act Article 13.
    """

    audit_id: str
    model_id: str
    model_version: str
    record_id: str = Field(description="Enregistrement sur lequel la décision a été prise")
    decision: str = Field(description="Décision prise ex: prime_elevee, sinistre_accepte")
    risk_score: float = Field(ge=0.0, le=1.0)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    input_features: dict[str, Any] = Field(description="Features utilisées — sans PII si possible")
    mitigation_appliquee: str | None = None
    ai_act_compliance_flag: Literal["compliant", "non_compliant", "under_review"] = "under_review"
    reviewed_by_human: bool = False
    timestamp: datetime = Field(default_factory=datetime.utcnow)
