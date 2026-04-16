"""
Dimension 4 — Precision (Précision numérique)

Logique : Vérifier le nombre de décimales pour les champs financiers.

  Montants assurance (primes, sinistres) :
    V  si nb_decimales >= 2
    IV si nb_decimales < 2

  Taux de change (données FX) :
    V  si nb_decimales >= 6
    S  si nb_decimales == 5
    IV si nb_decimales < 5

  Scores risque (0.0 à 1.0) :
    V  si nb_decimales >= 4
    IV si nb_decimales < 2
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from findata_dq.dimensions.base import BaseDimension
from findata_dq.models.dq_result import DQResult, DQStatus, ImpactLevel

# Règles de précision : field_pattern → (type, v_min, s_min)
# type : "insurance" | "fx" | "risk_score"
_PRECISION_RULES: dict[str, tuple[str, int, int]] = {
    # Montants assurance — 2 décimales minimum
    "prime_annuelle":      ("insurance", 2, 2),
    "montant_assure":      ("insurance", 2, 2),
    "montant_reclame":     ("insurance", 2, 2),
    "montant_rembourse":   ("insurance", 2, 2),
    "franchise":           ("insurance", 2, 2),
    "valeur_estimee":      ("insurance", 2, 2),

    # Scores risque — 4 décimales pour la précision actuarielle
    "score_risque_client": ("risk_score", 4, 2),
    "drift_score":         ("risk_score", 4, 2),
    "accuracy":            ("risk_score", 4, 2),
    "disparate_impact_sexe": ("risk_score", 4, 2),
    "auc_roc":             ("risk_score", 4, 2),
    "anomaly_score":       ("risk_score", 4, 2),
}


def _count_decimals(value: Any) -> int:
    """Compte le nombre de décimales significatives d'une valeur numérique."""
    try:
        d = Decimal(str(value))
        # Normaliser pour enlever les zéros trailing inutiles
        sign, digits, exponent = d.as_tuple()
        if exponent >= 0:
            return 0
        return -exponent
    except (InvalidOperation, TypeError, ValueError):
        return -1  # valeur non parseable


class Precision(BaseDimension):
    """
    Dimension 4 — Precision.
    Vérifie que les champs financiers et les scores ont le bon nombre de décimales.
    """

    name = "Precision"
    description = "Vérifie la précision numérique (nb de décimales) des champs financiers et scores."
    default_impact = ImpactLevel.MEDIUM

    def validate(
        self,
        record: dict[str, Any],
        config: dict[str, Any] = {},
    ) -> list[DQResult]:
        """
        Paramètres config :
          fields          : list[str]             — champs à tester (écrase le défaut)
          custom_rules    : dict[str, tuple]       — {field: (type, v_min_dec, s_min_dec)}
          dataset         : str
        """
        results: list[DQResult] = []

        # Règles actives (base + customs)
        rules = dict(_PRECISION_RULES)
        rules.update(config.get("custom_rules", {}))

        # Champs à tester : ceux fournis dans config, ou tous les champs du record qui ont une règle
        fields_to_check: list[str] = config.get("fields", [
            f for f in record.keys() if f in rules
        ])

        for field in fields_to_check:
            if field not in rules:
                continue

            raw = record.get(field)
            if raw is None or str(raw).strip() in ("", "null", "none"):
                continue

            field_type, v_min, s_min = rules[field]
            nb_dec = _count_decimals(raw)

            if nb_dec == -1:
                # Valeur non-numérique → Conformity s'en charge
                continue

            # Classification selon le type
            if field_type == "fx":
                if nb_dec >= 6:
                    status, impact = DQStatus.VALID, ImpactLevel.LOW
                elif nb_dec == 5:
                    status, impact = DQStatus.SUSPECT, ImpactLevel.MEDIUM
                else:
                    status, impact = DQStatus.INVALID, ImpactLevel.MEDIUM
                rule = f"Taux de change '{field}': {nb_dec} décimales (V>=6, S=5, IV<5)"

            else:  # insurance ou risk_score
                if nb_dec >= v_min:
                    status, impact = DQStatus.VALID, ImpactLevel.LOW
                elif nb_dec >= s_min:
                    status, impact = DQStatus.SUSPECT, ImpactLevel.MEDIUM
                else:
                    status, impact = DQStatus.INVALID, ImpactLevel.MEDIUM
                rule = (
                    f"Précision '{field}' ({field_type}): {nb_dec} décimales "
                    f"(V>={v_min}, IV<{s_min})"
                )

            results.append(self._make_result(
                record=record,
                field_name=field,
                field_value=str(raw),
                status=status,
                impact=impact,
                rule_applied=rule,
                details={
                    "field_type": field_type,
                    "nb_decimales": nb_dec,
                    "v_min_decimales": v_min,
                    "s_min_decimales": s_min,
                    "raw_value": str(raw),
                },
            ))

        return results
