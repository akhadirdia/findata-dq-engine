"""
Dimension 12 — PrivacyCompliance (Conformité PII / Loi 25)

Logique :
  1. Détection PII non masquée (environnements non-prod)
     NAS canadien, email, téléphone CA, carte de crédit
     → IV si PII détectée en dev/staging, Impact H

  2. Consentement
     → IV si id_client dans la table de refus, Impact H

  3. Rétention des données
     age_donnee = date_today - date_creation
     → IV si age_donnee > retention_max_jours, Impact H

  4. Pseudonymisation
     → S si id_client non haché dans les exports analytiques, Impact M
"""

from __future__ import annotations

import os
import re
from datetime import date, datetime, timezone
from typing import Any

from findata_dq.dimensions.base import BaseDimension
from findata_dq.models.dq_result import DQResult, DQStatus, ImpactLevel

# Patterns PII (CLAUDE.md section 4 — Dimension 12)
_PII_PATTERNS: dict[str, re.Pattern] = {
    "nas_canadien":   re.compile(r"\b\d{3}[-\s]?\d{3}[-\s]?\d{3}\b"),
    "email":          re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"),
    "telephone_ca":   re.compile(r"(\+1[-\s]?)?\(?\d{3}\)?[-\s]?\d{3}[-\s]?\d{4}"),
    "carte_credit":   re.compile(r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b"),
}

# Environnements où la PII non masquée est une violation
_NON_PROD_ENVS = {"development", "dev", "staging", "test", "qa"}

# Rétention par défaut par type de données (jours)
_DEFAULT_RETENTION: dict[str, int] = {
    "policies": 365 * 10,   # 10 ans (exigence réglementaire assurance)
    "claims": 365 * 10,
    "logs": 365 * 2,        # 2 ans (logs de sécurité — Loi 25)
    "model_metadata": 365 * 5,
    "clients": 365 * 7,
    "default": 365 * 5,
}

# Pattern d'id haché (SHA-256 = 64 hex, SHA-1 = 40 hex, MD5 = 32 hex)
_HASH_PATTERN = re.compile(r"^[a-fA-F0-9]{32,64}$")


def _to_date(value: Any) -> date | None:
    if value is None or str(value).strip() in ("", "null", "none"):
        return None
    s = str(value).strip()
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


class Privacy(BaseDimension):
    """
    Dimension 12 — PrivacyCompliance.
    Détecte les violations PII, consentement, rétention et pseudonymisation.
    Référence : Loi 25 Québec · RGPD Article 22 · AI Act Article 10(3).
    """

    name = "Privacy"
    description = "Détecte les violations PII, consentement, rétention et pseudonymisation (Loi 25 / RGPD)."
    default_impact = ImpactLevel.HIGH

    def validate(
        self,
        record: dict[str, Any],
        config: dict[str, Any] = {},
    ) -> list[DQResult]:
        """
        Paramètres config :
          pipeline_env       : str        — 'development'|'staging'|'production'
          pii_fields         : list[str]  — champs à scanner pour la PII
          refused_client_ids : set[str]   — clients ayant retiré leur consentement
          retention_days     : int        — rétention max en jours (écrase le défaut)
          check_pseudonymization : bool   — vérifier si id_client est haché (défaut True)
          dataset            : str
          reference_dt       : date       — date de référence pour la rétention
        """
        dataset = record.get("dataset", config.get("dataset", "unknown"))
        env = (
            record.get("pipeline_env")
            or config.get("pipeline_env")
            or os.getenv("PIPELINE_ENV", "development")
        ).lower()
        today = config.get("reference_dt", date.today())
        results: list[DQResult] = []

        # ── 1. Détection PII non masquée ─────────────────────────────────
        if env in _NON_PROD_ENVS:
            pii_fields: list[str] = config.get("pii_fields", [
                "revenu_estime", "ip_address", "source_ip",
                "cause_sinistre", "expert_assigne",
            ])
            for field in pii_fields:
                value = record.get(field)
                if value is None:
                    continue
                str_val = str(value)
                for pii_type, pattern in _PII_PATTERNS.items():
                    if pattern.search(str_val):
                        results.append(self._make_result(
                            record=record,
                            field_name=field,
                            field_value="[DETECTED]",
                            status=DQStatus.INVALID,
                            impact=ImpactLevel.HIGH,
                            rule_applied=(
                                f"PII détectée ({pii_type}) dans le champ '{field}' "
                                f"en environnement non-prod '{env}' — violation Loi 25."
                            ),
                            details={
                                "check": "pii_detection",
                                "pii_type": pii_type,
                                "field": field,
                                "env": env,
                            },
                        ))
                        break  # un seul résultat par champ

        # ── 2. Consentement ───────────────────────────────────────────────
        refused_ids: set = config.get("refused_client_ids", set())
        if refused_ids:
            id_client = record.get("id_client")
            if id_client and str(id_client) in refused_ids:
                results.append(self._make_result(
                    record=record,
                    field_name="id_client",
                    field_value="[MASKED]",
                    status=DQStatus.INVALID,
                    impact=ImpactLevel.HIGH,
                    rule_applied=(
                        "Consentement retiré : id_client figure dans la table de refus — "
                        "traitement interdit (Loi 25 / RGPD Art. 17)."
                    ),
                    details={"check": "consent", "id_client": "[MASKED]"},
                ))

        # ── 3. Rétention des données ──────────────────────────────────────
        date_creation = _to_date(record.get("date_creation"))
        if date_creation:
            retention_max = config.get(
                "retention_days",
                _DEFAULT_RETENTION.get(dataset, _DEFAULT_RETENTION["default"]),
            )
            age_jours = (today - date_creation).days

            if age_jours > retention_max:
                results.append(self._make_result(
                    record=record,
                    field_name="date_creation",
                    field_value=str(date_creation),
                    status=DQStatus.INVALID,
                    impact=ImpactLevel.HIGH,
                    rule_applied=(
                        f"Rétention dépassée : donnée âgée de {age_jours} jours > "
                        f"limite {retention_max} jours — suppression requise (Loi 25)."
                    ),
                    details={
                        "check": "retention",
                        "age_jours": age_jours,
                        "retention_max_jours": retention_max,
                        "date_creation": str(date_creation),
                    },
                ))
            else:
                results.append(self._make_result(
                    record=record,
                    field_name="date_creation",
                    field_value=str(date_creation),
                    status=DQStatus.VALID,
                    impact=ImpactLevel.LOW,
                    rule_applied=f"Rétention conforme ({age_jours}/{retention_max} jours).",
                    details={"check": "retention", "age_jours": age_jours},
                ))

        # ── 4. Pseudonymisation ───────────────────────────────────────────
        if config.get("check_pseudonymization", False):
            id_client = record.get("id_client")
            if id_client:
                str_id = str(id_client)
                is_hashed = bool(_HASH_PATTERN.match(str_id))
                if not is_hashed:
                    results.append(self._make_result(
                        record=record,
                        field_name="id_client",
                        field_value="[NOT_HASHED]",
                        status=DQStatus.SUSPECT,
                        impact=ImpactLevel.MEDIUM,
                        rule_applied=(
                            "Pseudonymisation : id_client non haché dans les exports analytiques — "
                            "risque de réidentification (Loi 25 Art. 23)."
                        ),
                        details={"check": "pseudonymization", "id_format": str_id[:6] + "..."},
                    ))

        return results
