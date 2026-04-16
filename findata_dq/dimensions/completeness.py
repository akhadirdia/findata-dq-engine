"""
Dimension 1 — Completeness (Complétude)

Logique : Détecter les valeurs nulles, vides, ou placeholders sur les champs requis.

  M (Mandatory) + null/vide → IV, Impact H
  O (Optional)  + null/vide → S,  Impact L

  completeness_rate = (nb_lignes - nb_nulls) / nb_lignes * 100
  V  si completeness_rate = 100%
  S  si completeness_rate entre 95% et 100%
  IV si completeness_rate < 95%
"""

from __future__ import annotations

from typing import Any

from findata_dq.dimensions.base import BaseDimension
from findata_dq.models.dq_result import DQResult, DQStatus, ImpactLevel

# Valeurs considérées comme "vides" en plus de None
_EMPTY_PLACEHOLDERS = {"", "null", "none", "n/a", "na", "?", "-", "unknown"}

# Champs obligatoires par dataset (défaut — surchargeable via config)
DEFAULT_MANDATORY: dict[str, list[str]] = {
    "policies": [
        "num_police", "id_client", "date_effet", "date_expiration",
        "type_couverture", "prime_annuelle", "montant_assure",
    ],
    "claims": [
        "id_sinistre", "num_police", "id_client", "date_sinistre",
        "montant_reclame", "type_dommage", "statut_sinistre",
    ],
    "logs": ["log_id", "timestamp", "user_id", "action_type", "status_code"],
    "model_metadata": [
        "model_id", "model_name", "model_version", "training_date", "statut_production",
    ],
}


def _is_empty(value: Any) -> bool:
    """Retourne True si la valeur est considérée comme manquante."""
    if value is None:
        return True
    if isinstance(value, str) and value.strip().lower() in _EMPTY_PLACEHOLDERS:
        return True
    return False


class Completeness(BaseDimension):
    """
    Dimension 1 — Completeness.
    Vérifie que tous les champs obligatoires sont présents et non vides.
    """

    name = "Completeness"
    description = "Détecte les valeurs nulles, vides ou placeholders sur les champs requis."
    default_impact = ImpactLevel.HIGH

    def validate(
        self,
        record: dict[str, Any],
        config: dict[str, Any] = {},
    ) -> list[DQResult]:
        """
        Paramètres config :
          mandatory_fields : list[str]  — champs obligatoires (écrase DEFAULT_MANDATORY)
          optional_fields  : list[str]  — champs optionnels à surveiller
          dataset          : str        — nom du dataset (pour les defaults)
        """
        dataset = record.get("dataset", config.get("dataset", "unknown"))
        results: list[DQResult] = []

        # Résolution des champs à tester
        mandatory: list[str] = config.get(
            "mandatory_fields",
            DEFAULT_MANDATORY.get(dataset, []),
        )
        optional: list[str] = config.get("optional_fields", [])

        # Vérification des champs obligatoires
        for field in mandatory:
            value = record.get(field)
            if _is_empty(value):
                results.append(self._make_result(
                    record=record,
                    field_name=field,
                    field_value=value,
                    status=DQStatus.INVALID,
                    impact=ImpactLevel.HIGH,
                    rule_applied=f"Champ obligatoire '{field}' absent ou vide.",
                    details={"field_type": "mandatory", "raw_value": str(value)},
                ))
            else:
                results.append(self._make_result(
                    record=record,
                    field_name=field,
                    field_value=value,
                    status=DQStatus.VALID,
                    impact=ImpactLevel.LOW,
                    rule_applied=f"Champ obligatoire '{field}' présent.",
                    details={"field_type": "mandatory"},
                ))

        # Vérification des champs optionnels
        for field in optional:
            value = record.get(field)
            if _is_empty(value):
                results.append(self._make_result(
                    record=record,
                    field_name=field,
                    field_value=value,
                    status=DQStatus.SUSPECT,
                    impact=ImpactLevel.LOW,
                    rule_applied=f"Champ optionnel '{field}' absent — surveillance.",
                    details={"field_type": "optional", "raw_value": str(value)},
                ))
            else:
                results.append(self._make_result(
                    record=record,
                    field_name=field,
                    field_value=value,
                    status=DQStatus.VALID,
                    impact=ImpactLevel.LOW,
                    rule_applied=f"Champ optionnel '{field}' présent.",
                    details={"field_type": "optional"},
                ))

        return results
