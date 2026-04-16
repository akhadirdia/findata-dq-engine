"""
Dimension 7 — Collection (Intégrité de l'ensemble)

Logique : Un ensemble de données doit être complet (tous les éléments attendus sont présents).

  ecart = |record_count_recu - record_count_attendu| / record_count_attendu * 100
  V  si ecart < 1%
  S  si 1% <= ecart < 3%
  IV si ecart >= 3%, Impact H

Contrôle additionnel — Somme de contrôle :
  somme_primes_calculee vs somme_primes_attendue → même règle 1%/3%
"""

from __future__ import annotations

from typing import Any

from findata_dq.dimensions.base import BaseDimension
from findata_dq.models.dq_result import DQResult, DQStatus, ImpactLevel

# Seuils
_V_PCT = 1.0
_IV_PCT = 3.0


class Collection(BaseDimension):
    """
    Dimension 7 — Collection.
    Vérifie que l'ensemble de données reçu est complet par rapport à l'attendu.

    Note : cette dimension opère au niveau du DATASET entier, pas ligne par ligne.
    Le record passé à validate() représente un résumé agrégé du dataset.
    """

    name = "Collection"
    description = "Vérifie que le nombre d'enregistrements et les sommes de contrôle correspondent aux attendus."
    default_impact = ImpactLevel.HIGH

    def validate(
        self,
        record: dict[str, Any],
        config: dict[str, Any] = {},
    ) -> list[DQResult]:
        """
        Paramètres config (REQUIS) :
          count_checks : list[dict]  — vérifications de nombre d'enregistrements
            Chaque dict : {
              "name"           : str,    # libellé lisible
              "received"       : int,    # nb lignes reçues
              "expected"       : int,    # nb lignes attendues (registre officiel)
            }

          sum_checks : list[dict]  — vérifications de sommes de contrôle
            Chaque dict : {
              "name"           : str,
              "computed_sum"   : float,
              "expected_sum"   : float,
              "field"          : str,    # champ de référence pour DQResult.field_name
            }

          dataset : str

        Note : record doit contenir au minimum {"record_id": "dataset_summary", "dataset": ...}
        """
        results: list[DQResult] = []

        # ── Vérifications du nombre d'enregistrements ─────────────────────
        for check in config.get("count_checks", []):
            name: str = check["name"]
            received: int = int(check["received"])
            expected: int = int(check["expected"])

            if expected == 0:
                continue

            ecart_pct = abs(received - expected) / expected * 100

            if ecart_pct < _V_PCT:
                status = DQStatus.VALID
                impact = ImpactLevel.LOW
            elif ecart_pct < _IV_PCT:
                status = DQStatus.SUSPECT
                impact = ImpactLevel.MEDIUM
            else:
                status = DQStatus.INVALID
                impact = ImpactLevel.HIGH

            results.append(self._make_result(
                record=record,
                field_name=f"record_count_{name}",
                status=status,
                impact=impact,
                rule_applied=(
                    f"Collection '{name}': {received} reçus / {expected} attendus "
                    f"(écart {ecart_pct:.2f}% — V<{_V_PCT}%, S<{_IV_PCT}%, IV>={_IV_PCT}%)"
                ),
                details={
                    "check_name": name,
                    "received": received,
                    "expected": expected,
                    "ecart_pct": round(ecart_pct, 4),
                    "v_threshold_pct": _V_PCT,
                    "iv_threshold_pct": _IV_PCT,
                },
            ))

        # ── Vérifications des sommes de contrôle ──────────────────────────
        for check in config.get("sum_checks", []):
            name = check["name"]
            computed = float(check["computed_sum"])
            expected = float(check["expected_sum"])
            field = check.get("field", f"sum_{name}")

            if expected == 0:
                continue

            ecart_pct = abs(computed - expected) / abs(expected) * 100

            if ecart_pct < _V_PCT:
                status = DQStatus.VALID
                impact = ImpactLevel.LOW
            elif ecart_pct < _IV_PCT:
                status = DQStatus.SUSPECT
                impact = ImpactLevel.MEDIUM
            else:
                status = DQStatus.INVALID
                impact = ImpactLevel.HIGH

            results.append(self._make_result(
                record=record,
                field_name=field,
                status=status,
                impact=impact,
                rule_applied=(
                    f"Somme de contrôle '{name}': calculée={computed:,.2f} / attendue={expected:,.2f} "
                    f"(écart {ecart_pct:.2f}%)"
                ),
                details={
                    "check_name": name,
                    "computed_sum": computed,
                    "expected_sum": expected,
                    "ecart_pct": round(ecart_pct, 4),
                },
            ))

        return results
