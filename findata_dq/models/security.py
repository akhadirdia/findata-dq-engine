"""
Modèles Pydantic pour les données de sécurité et fraude.
Domaine B : logs applicatifs, transactions, menaces.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

# ─── Log applicatif ───────────────────────────────────────────────────────────

class AccessLog(BaseModel):
    """
    Représente un événement applicatif loggé.
    Dataset : access_logs.csv
    Utilisé pour : détection d'intrusion, Congruence, Isolation Forest.
    """

    log_id: str = Field(description="Identifiant unique du log")
    timestamp: datetime = Field(description="Horodatage UTC de l'événement — critique pour Timeliness")
    user_id: str = Field(description="Identifiant utilisateur — référence vers table users")
    ip_address: str = Field(description="Adresse IP source — PII, format IPv4")
    action_type: Literal["login", "logout", "read", "write", "delete", "transfer", "export"]
    session_id: str | None = Field(default=None, description="Identifiant de session")
    device_type: Literal["web", "mobile", "api", "desktop", "unknown"] = "unknown"
    status_code: int = Field(description="Code HTTP de réponse (200, 401, 403, 500...)")
    payload_size: int | None = Field(default=None, ge=0, description="Taille en bytes")
    anomaly_score: float | None = Field(
        default=None,
        description="Score d'anomalie pré-calculé ou généré par Isolation Forest"
    )
    date_creation: datetime | None = None

    @property
    def is_suspicious_action(self) -> bool:
        """Heuristique rapide — compléter avec Isolation Forest."""
        return self.action_type in ("delete", "export") and self.status_code != 200

    @property
    def is_failed_auth(self) -> bool:
        return self.action_type == "login" and self.status_code == 401


# ─── Transaction financière ───────────────────────────────────────────────────

class Transaction(BaseModel):
    """
    Représente une transaction financière (paiement de prime, remboursement sinistre).
    Dataset : transactions.csv
    Utilisé pour : détection de fraude, Congruence Z-score sur montants.
    """

    transaction_id: str
    user_id: str = Field(description="Référence vers table users")
    num_police: str | None = Field(default=None, description="Police associée si applicable")
    id_sinistre: str | None = Field(default=None, description="Sinistre associé si applicable")
    montant: float = Field(description="Montant en CAD — positif=crédit, négatif=débit")
    type_transaction: Literal[
        "paiement_prime", "remboursement_sinistre", "remboursement_trop_percu",
        "frais_expertise", "virement_interne", "ajustement"
    ]
    canal: Literal["web", "app", "telephone", "agence", "virement_bancaire"]
    heure_transaction: datetime
    statut: Literal["valide", "en_attente", "rejete", "en_cours", "annule"]
    flag_fraude: bool | None = Field(default=None, description="Label fraude confirmée (pour entraînement)")
    ip_address: str | None = None
    date_creation: datetime | None = None

    @property
    def is_high_value(self) -> bool:
        return abs(self.montant) > 50_000


# ─── Log de menace / Threat ───────────────────────────────────────────────────

class ThreatLog(BaseModel):
    """
    Représente un événement de sécurité réseau.
    Dataset : threat_logs.csv
    Utilisé pour : détection d'intrusion, scoring cybersécurité ESG.
    """

    threat_id: str
    timestamp: datetime
    source_ip: str = Field(description="IP source — PII potentielle")
    destination_ip: str
    payload_size: int = Field(ge=0)
    protocol: Literal["HTTP", "HTTPS", "SSH", "FTP", "DNS", "SMTP", "OTHER"]
    anomaly_score: float = Field(ge=0.0, le=1.0, description="Score calculé par SIEM ou notre pipeline")
    threat_category: Literal["brute_force", "sql_injection", "xss", "ddos", "data_exfiltration", "privilege_escalation", "unknown"] | None = None
    blocked: bool = False
    date_creation: datetime | None = None
