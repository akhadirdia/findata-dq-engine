"""
Dimension 3 — Accuracy (Exactitude)

Deux logiques (CLAUDE.md section 4) :

1. Source d'autorité : Comparaison avec une table de référence officielle.
   valeur IN reference_table → V
   valeur NOT IN             → IV, Impact H

2. Triangulation mathématique : Vérification par calcul indirect.
   ratio_sinistre_prime = montant_reclame / prime_annuelle  → vérifiable
   position_calculee = nb_sinistres_ouverts - nb_sinistres_fermes → doit matcher
   prime_totale_portefeuille = SUM(prime_annuelle) → tolérance 1%
"""

from __future__ import annotations

from typing import Any

from findata_dq.dimensions.base import BaseDimension
from findata_dq.models.dq_result import DQResult, DQStatus, ImpactLevel

# Tolérance pour les triangulations mathématiques
_TRIANGULATION_TOLERANCE = 0.01  # 1%


class Accuracy(BaseDimension):
    """
    Dimension 3 — Accuracy.
    Vérifie l'exactitude des données par rapport à une source d'autorité
    ou par triangulation mathématique.
    """

    name = "Accuracy"
    description = "Vérifie l'exactitude par source d'autorité et triangulation mathématique."
    default_impact = ImpactLevel.HIGH

    def validate(
        self,
        record: dict[str, Any],
        config: dict[str, Any] = {},
    ) -> list[DQResult]:
        """
        Paramètres config :
          authority_checks : list[dict]  — vérifications par source d'autorité
            Chaque dict : {
              "field"     : str,           # champ à tester
              "reference" : set | dict,    # valeurs valides ou {value: canonical}
              "impact"    : str            # "H" | "M" | "L"
            }

          triangulations : list[dict]  — vérifications mathématiques
            Chaque dict : {
              "name"      : str,      # libellé lisible
              "computed"  : float,    # valeur calculée
              "declared"  : float,    # valeur déclarée dans le record
              "tolerance" : float,    # tolérance relative (défaut 0.01 = 1%)
              "impact"    : str
            }

          dataset : str
        """
        results: list[DQResult] = []

        # ── 1. Source d'autorité ───────────────────────────────────────────
        for check in config.get("authority_checks", []):
            field: str = check["field"]
            reference: set | dict = check["reference"]
            impact: str = check.get("impact", ImpactLevel.HIGH)

            value = record.get(field)
            if value is None or str(value).strip() == "":
                continue

            str_value = str(value).strip()

            if isinstance(reference, dict):
                valid_keys = set(reference.keys())
            else:
                valid_keys = set(str(v) for v in reference)

            if str_value in valid_keys:
                canonical = reference.get(str_value, str_value) if isinstance(reference, dict) else str_value
                results.append(self._make_result(
                    record=record,
                    field_name=field,
                    field_value=str_value,
                    status=DQStatus.VALID,
                    impact=ImpactLevel.LOW,
                    rule_applied=f"Valeur '{field}' confirmée par la source d'autorité.",
                    details={"canonical_value": str(canonical), "ref_size": len(valid_keys)},
                ))
            else:
                results.append(self._make_result(
                    record=record,
                    field_name=field,
                    field_value=str_value,
                    status=DQStatus.INVALID,
                    impact=impact,
                    rule_applied=(
                        f"Valeur '{field}' = '{str_value}' absente de la source d'autorité "
                        f"({len(valid_keys)} valeurs valides)."
                    ),
                    details={"value": str_value, "ref_size": len(valid_keys)},
                ))

        # ── 2. Triangulations mathématiques ───────────────────────────────
        for tri in config.get("triangulations", []):
            name: str = tri["name"]
            computed: float = float(tri["computed"])
            declared: float = float(tri["declared"])
            tolerance: float = float(tri.get("tolerance", _TRIANGULATION_TOLERANCE))
            impact: str = tri.get("impact", ImpactLevel.HIGH)

            if declared == 0:
                # Éviter la division par zéro — écart absolu
                delta = abs(computed - declared)
                ok = delta <= tolerance
            else:
                relative_delta = abs(computed - declared) / abs(declared)
                ok = relative_delta <= tolerance
                delta = relative_delta

            field_name = tri.get("field", name)

            if ok:
                results.append(self._make_result(
                    record=record,
                    field_name=field_name,
                    status=DQStatus.VALID,
                    impact=ImpactLevel.LOW,
                    rule_applied=f"Triangulation '{name}' : cohérente (écart {delta:.4f} <= tolérance {tolerance}).",
                    details={
                        "triangulation": name,
                        "computed": computed,
                        "declared": declared,
                        "relative_delta": round(delta, 6),
                        "tolerance": tolerance,
                    },
                ))
            else:
                results.append(self._make_result(
                    record=record,
                    field_name=field_name,
                    status=DQStatus.INVALID,
                    impact=impact,
                    rule_applied=(
                        f"Triangulation '{name}' incohérente : "
                        f"calculé={computed:.4f} vs déclaré={declared:.4f} "
                        f"(écart {delta:.2%} > tolérance {tolerance:.2%})."
                    ),
                    details={
                        "triangulation": name,
                        "computed": computed,
                        "declared": declared,
                        "relative_delta": round(delta, 6),
                        "tolerance": tolerance,
                    },
                ))

        return results
