"""
Dimension 5 — Conformity (Conformité de format)

Logique : Validation par Regex des formats standards.
  format valide   → V
  format invalide → IV, Impact selon le champ (H pour num_police, M pour code_postal)
"""

from __future__ import annotations

import re
from typing import Any

from findata_dq.dimensions.base import BaseDimension
from findata_dq.models.dq_result import DQResult, DQStatus, ImpactLevel

# Patterns définis en CLAUDE.md section 4 — Dimension 5
_PATTERNS: dict[str, tuple[str, str]] = {
    # (regex, impact_si_invalide)
    "num_police":       (r"^[A-Z]{2,3}-\d{6,10}$",                     ImpactLevel.HIGH),
    "type_couverture":  (r"^(auto|habitation|vie|sante|entreprise)$",   ImpactLevel.MEDIUM),
    "statut_sinistre":  (r"^(ouvert|ferme|en_cours|rejete|paye)$",      ImpactLevel.MEDIUM),
    "statut_police":    (r"^(active|expiree|suspendue|resiliee)$",       ImpactLevel.MEDIUM),
    "code_postal_ca":   (r"^[A-Z]\d[A-Z]\s?\d[A-Z]\d$",                ImpactLevel.MEDIUM),
    "code_postal_lieu": (r"^[A-Z]\d[A-Z]\s?\d[A-Z]\d$",                ImpactLevel.MEDIUM),
    "code_postal":      (r"^[A-Z]\d[A-Z]\s?\d[A-Z]\d$",                ImpactLevel.MEDIUM),
    "numero_vin":       (r"^[A-HJ-NPR-Z0-9]{17}$",                     ImpactLevel.HIGH),
    "ip_address":       (r"^((25[0-5]|2[0-4]\d|1\d\d|[1-9]\d|\d)\.){3}(25[0-5]|2[0-4]\d|1\d\d|[1-9]\d|\d)$", ImpactLevel.MEDIUM),
    "source_ip":        (r"^((25[0-5]|2[0-4]\d|1\d\d|[1-9]\d|\d)\.){3}(25[0-5]|2[0-4]\d|1\d\d|[1-9]\d|\d)$", ImpactLevel.MEDIUM),
    "action_type":      (r"^(login|logout|read|write|delete|transfer|export)$", ImpactLevel.MEDIUM),
    "model_version":    (r"^\d+\.\d+\.\d+$",                           ImpactLevel.MEDIUM),
    "ai_act_compliance_flag": (r"^(compliant|non_compliant|under_review)$", ImpactLevel.HIGH),
    "statut_production": (
        r"^(en_dev|staging|production|retire|archive)$",
        ImpactLevel.HIGH,
    ),
}

# Champs à vérifier par dataset (défaut — surchargeable via config)
DEFAULT_FIELDS_BY_DATASET: dict[str, list[str]] = {
    "policies": ["num_police", "type_couverture", "statut_police", "code_postal"],
    "claims": ["statut_sinistre", "code_postal_lieu"],
    "logs": ["action_type", "ip_address"],
    "model_metadata": ["model_version", "ai_act_compliance_flag", "statut_production"],
}


class Conformity(BaseDimension):
    """
    Dimension 5 — Conformity.
    Valide le format de chaque champ via une expression régulière.
    """

    name = "Conformity"
    description = "Valide le format des champs par regex (codes, identifiants, énumérations)."
    default_impact = ImpactLevel.MEDIUM

    def validate(
        self,
        record: dict[str, Any],
        config: dict[str, Any] | None = None,
    ) -> list[DQResult]:
        config = config or {}
        """
        Paramètres config :
          fields          : list[str]              — champs à tester (écrase le défaut par dataset)
          custom_patterns : dict[str, str]         — patterns supplémentaires {field: regex}
          dataset         : str                    — nom du dataset
        """
        dataset = record.get("dataset", config.get("dataset", "unknown"))
        results: list[DQResult] = []

        # Champs à tester
        fields_to_check: list[str] = config.get(
            "fields",
            DEFAULT_FIELDS_BY_DATASET.get(dataset, list(_PATTERNS.keys())),
        )

        # Patterns personnalisés (override ou ajout)
        patterns = dict(_PATTERNS)
        for field, regex in config.get("custom_patterns", {}).items():
            patterns[field] = (regex, ImpactLevel.MEDIUM)

        for field in fields_to_check:
            value = record.get(field)

            # Champ absent ou None → pas de vérification de format (Completeness s'en charge)
            if value is None or str(value).strip() == "":
                continue

            str_value = str(value).strip()

            if field not in patterns:
                continue  # pas de règle connue pour ce champ

            regex, impact = patterns[field]

            if re.match(regex, str_value):
                results.append(self._make_result(
                    record=record,
                    field_name=field,
                    field_value=str_value,
                    status=DQStatus.VALID,
                    impact=ImpactLevel.LOW,
                    rule_applied=f"Format '{field}' conforme au pattern attendu.",
                    details={"pattern": regex, "value": str_value},
                ))
            else:
                results.append(self._make_result(
                    record=record,
                    field_name=field,
                    field_value=str_value,
                    status=DQStatus.INVALID,
                    impact=impact,
                    rule_applied=f"Format '{field}' invalide — attendu : {regex}",
                    details={"pattern": regex, "value": str_value},
                ))

        return results
