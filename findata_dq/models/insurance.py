"""
Modèles Pydantic pour les données d'assurance de dommages.
Domaine A : polices, sinistres, clients, véhicules.
"""

from __future__ import annotations

from datetime import date
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ─── Police / Contrat ─────────────────────────────────────────────────────────

class Policy(BaseModel):
    """
    Représente un contrat d'assurance.
    Dataset : policies.csv
    """

    num_police: str = Field(description="Identifiant unique de la police (ex: AU-123456)")
    id_client: str = Field(description="Référence vers Client.id_client")
    date_effet: date = Field(description="Date de début de couverture")
    date_expiration: date = Field(description="Date de fin de couverture")
    type_couverture: Literal["auto", "habitation", "vie", "sante", "entreprise"]
    prime_annuelle: float = Field(gt=0, description="Prime annuelle en CAD")
    montant_assure: float = Field(gt=0, description="Montant maximum couvert en CAD")
    statut_police: Literal["active", "expiree", "suspendue", "resiliee"] = "active"
    franchise: float = Field(default=0.0, ge=0)
    date_creation: Optional[date] = None

    @model_validator(mode="after")
    def validate_dates(self) -> "Policy":
        if self.date_expiration <= self.date_effet:
            raise ValueError("date_expiration doit être postérieure à date_effet")
        return self

    @property
    def is_active(self) -> bool:
        from datetime import date as today_cls
        return (
            self.statut_police == "active"
            and self.date_effet <= today_cls.today() <= self.date_expiration
        )


# ─── Sinistre / Claim ─────────────────────────────────────────────────────────

class Claim(BaseModel):
    """
    Représente un sinistre déclaré.
    Dataset : claims.csv
    """

    id_sinistre: str = Field(description="Identifiant unique du sinistre (ex: SIN-2024-001234)")
    num_police: str = Field(description="Référence vers Policy.num_police")
    id_client: str = Field(description="Référence vers Client.id_client")
    date_sinistre: date = Field(description="Date de survenance du sinistre")
    date_declaration: Optional[date] = Field(default=None, description="Date de déclaration à l'assureur")
    montant_reclame: float = Field(gt=0, description="Montant réclamé en CAD")
    montant_rembourse: Optional[float] = Field(default=None, ge=0)
    type_dommage: Literal[
        "collision", "vol", "incendie", "degats_eau", "bris_glace",
        "responsabilite_civile", "vandalisme", "catastrophe_naturelle", "autre"
    ]
    cause_sinistre: str = Field(description="Description libre de la cause")
    statut_sinistre: Literal["ouvert", "ferme", "en_cours", "rejete", "paye"]
    code_postal_lieu: str = Field(description="Code postal du lieu du sinistre (format canadien)")
    expert_assigne: Optional[str] = None
    date_creation: Optional[date] = None

    # Champs calculés (remplis après validation)
    delta_jours_declaration: Optional[int] = Field(
        default=None,
        description="Jours entre sinistre et déclaration — utilisé par Timeliness"
    )

    @field_validator("montant_rembourse")
    @classmethod
    def rembourse_coherent(cls, v: Optional[float]) -> Optional[float]:
        return v  # La vérification cross-champ se fait dans BusinessRules


# ─── Client / Assuré ──────────────────────────────────────────────────────────

class Client(BaseModel):
    """
    Représente un client assuré.
    Dataset : clients.csv
    Attention : contient des données sensibles (PII) — surveillance Privacy dimension.
    """

    id_client: str = Field(description="Identifiant unique client (haché en analytique)")
    age: int = Field(ge=16, le=120, description="Âge en années — attribut protégé Fairness")
    sexe: Literal["H", "F", "NB", "ND"] = Field(description="Attribut protégé Fairness")
    code_postal: str = Field(description="Code postal canadien — proxy socioéconomique")
    revenu_estime: Optional[float] = Field(default=None, description="PII — jamais en clair hors prod")
    historique_sinistres: int = Field(ge=0, description="Nombre de sinistres sur 5 ans")
    score_risque_client: float = Field(ge=0.0, le=1.0, description="Score de risque (output modèle)")
    consentement_analytique: bool = Field(default=False)
    date_creation: Optional[date] = None

    @property
    def is_high_risk(self) -> bool:
        return self.score_risque_client >= 0.7


# ─── Véhicule / Bien ─────────────────────────────────────────────────────────

class Vehicle(BaseModel):
    """
    Représente un véhicule ou bien assuré.
    Dataset : vehicles.csv
    """

    numero_vin: str = Field(
        min_length=17, max_length=17,
        description="Vehicle Identification Number — 17 caractères alphanumériques"
    )
    id_client: str = Field(description="Référence vers Client.id_client")
    num_police: str = Field(description="Référence vers Policy.num_police")
    marque: str
    modele: str
    annee_fabrication: int = Field(ge=1900, le=2030)
    valeur_estimee: float = Field(gt=0, description="Valeur marchande estimée en CAD")
    kilometrage: Optional[int] = Field(default=None, ge=0)
    usage: Literal["personnel", "commercial", "mixte"] = "personnel"
    date_creation: Optional[date] = None

    @property
    def age_vehicule(self) -> int:
        from datetime import date
        return date.today().year - self.annee_fabrication
