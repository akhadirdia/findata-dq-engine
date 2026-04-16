"""
Modèles Pydantic pour les données de risques non-financiers.
Domaine C : incidents opérationnels, ESG, conformité réglementaire.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field

# ─── Incident opérationnel ────────────────────────────────────────────────────

class Incident(BaseModel):
    """
    Représente un incident opérationnel interne.
    Dataset : incidents.csv
    Utilisé pour : scoring risque opérationnel, tableau de bord Power BI.
    """

    id_incident: str
    date_incident: date
    date_detection: date | None = None
    date_cloture: date | None = None
    categorie_risque: Literal[
        "processus", "ressources_humaines", "systemes_it",
        "fraude_interne", "fraude_externe", "juridique", "catastrophe"
    ]
    sous_categorie: str | None = None
    gravite: Literal["critique", "majeure", "moderee", "mineure"]
    statut: Literal["ouvert", "en_cours", "clos", "escalade"]
    impact_financier_estime: float | None = Field(
        default=None,
        description="Perte financière estimée en CAD — utilisé pour Accuracy triangulation"
    )
    impact_financier_reel: float | None = None
    owner_departement: str = Field(description="Département responsable de la résolution")
    description: str | None = None
    date_creation: datetime | None = None

    @property
    def delta_jours_resolution(self) -> int | None:
        if self.date_cloture and self.date_detection:
            return (self.date_cloture - self.date_detection).days
        return None

    @property
    def is_critical(self) -> bool:
        return self.gravite == "critique" and self.statut in ("ouvert", "escalade")


# ─── ESG / Cybersécurité ──────────────────────────────────────────────────────

class ESGRecord(BaseModel):
    """
    Représente un enregistrement de scoring ESG et cybersécurité.
    Dataset : esg_records.csv
    Utilisé pour : Dimension Fairness (ESG), reporting Comité des Risques.
    """

    record_id: str
    date_evaluation: date
    periode: str = Field(description="Période couverte ex: 2024-Q1")
    score_esg_global: float = Field(ge=0.0, le=100.0)
    score_environnemental: float | None = Field(default=None, ge=0.0, le=100.0)
    score_social: float | None = Field(default=None, ge=0.0, le=100.0)
    score_gouvernance: float | None = Field(default=None, ge=0.0, le=100.0)
    incident_cyber: bool = Field(description="Au moins un incident cyber dans la période")
    nombre_vulnerabilites: int = Field(ge=0, description="Vulnérabilités détectées et non corrigées")
    conformite_loi25: Literal["conforme", "non_conforme", "en_cours"] = Field(
        description="Statut de conformité Loi 25 Québec"
    )
    conformite_rgpd: Literal["conforme", "non_conforme", "en_cours"] | None = None
    date_creation: datetime | None = None


# ─── Contrôle de conformité ───────────────────────────────────────────────────

class AuditControl(BaseModel):
    """
    Représente un contrôle d'audit de conformité.
    Dataset : audit_controls.csv
    Utilisé pour : Dimension Cohesion (intégrité référentielle des contrôles).
    """

    id_controle: str
    nom_controle: str
    categorie: Literal[
        "financier", "operationnel", "it", "rh",
        "reglementaire", "protection_donnees"
    ]
    statut_audit: Literal["passe", "echoue", "en_attente", "non_applicable"]
    date_audit: date
    prochaine_date_audit: date | None = None
    owner_departement: str
    criticite: Literal["critique", "elevee", "moderee", "faible"]
    observations: str | None = None
    plan_action: str | None = None
    date_creation: datetime | None = None

    @property
    def is_overdue(self) -> bool:
        from datetime import date as today_cls
        if self.prochaine_date_audit:
            return today_cls.today() > self.prochaine_date_audit
        return False
