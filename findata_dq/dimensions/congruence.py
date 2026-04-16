"""
Dimension 6 — Congruence (Détection d'outliers)

Trois algorithmes (CLAUDE.md section 4) :

1. Prior Value Comparison (évolution jour-à-jour) :
   |valeur_J - valeur_J-1| / moyenne(J, J-1) * 100
   V < 10%, S < 20%, IV >= 20%

2. Comparison to Average (écart à la moyenne historique) :
   |valeur - moyenne_historique| / moyenne_historique * 100
   V < 5%, S < 15%, IV >= 15%

3. Z-Score (le plus robuste — fenêtre glissante 30 jours) :
   Z = (valeur - moyenne_30j) / ecart_type_30j
   V : |Z| <= 2, S : 2 < |Z| < 3.5, IV : |Z| >= 3.5
"""

from __future__ import annotations

from typing import Any

from findata_dq.dimensions.base import BaseDimension
from findata_dq.models.dq_result import DQResult, DQStatus, ImpactLevel

# Seuils Prior Value Comparison
_PVC_V = 10.0
_PVC_IV = 20.0

# Seuils Comparison to Average (assurance — plus tolérant que FX)
_CTA_V = 5.0
_CTA_IV = 15.0

# Champs numériques surveillés par défaut par dataset
_NUMERIC_FIELDS: dict[str, list[str]] = {
    "policies": ["prime_annuelle", "montant_assure", "franchise"],
    "claims": ["montant_reclame", "montant_rembourse"],
    "logs": ["payload_size", "anomaly_score"],
    "model_metadata": ["drift_score", "accuracy", "disparate_impact_sexe"],
}


class Congruence(BaseDimension):
    """
    Dimension 6 — Congruence.
    Détecte les outliers statistiques sur les champs numériques.
    Complétée par l'Isolation Forest (Couche 2) sur les données multivariées.
    """

    name = "Congruence"
    description = "Détecte les outliers numériques via Z-score, Prior Value et Comparison to Average."
    default_impact = ImpactLevel.HIGH

    def validate(
        self,
        record: dict[str, Any],
        config: dict[str, Any] | None = None,
    ) -> list[DQResult]:
        config = config or {}
        """
        Paramètres config (au moins un algorithme doit être fourni) :
          fields          : list[str]         — champs à tester
          dataset         : str

          # Algorithme 1 — Prior Value Comparison
          prior_values    : dict[str, float]  — {field: valeur_J-1}

          # Algorithme 2 — Comparison to Average
          historical_means: dict[str, float]  — {field: moyenne_historique}

          # Algorithme 3 — Z-Score (recommandé)
          zscore_stats    : dict[str, dict]   — {field: {"mean": x, "std": y}}

        Si aucun stats n'est fourni, la dimension retourne une liste vide.
        """
        dataset = record.get("dataset", config.get("dataset", "unknown"))
        results: list[DQResult] = []

        fields: list[str] = config.get(
            "fields",
            _NUMERIC_FIELDS.get(dataset, []),
        )

        prior_values: dict[str, float] = config.get("prior_values", {})
        historical_means: dict[str, float] = config.get("historical_means", {})
        zscore_stats: dict[str, dict] = config.get("zscore_stats", {})

        for field in fields:
            raw = record.get(field)
            if raw is None or str(raw).strip() == "":
                continue
            try:
                value = float(raw)
            except (ValueError, TypeError):
                continue

            # ── Algorithme 3 — Z-Score (priorité) ─────────────────────────
            if field in zscore_stats:
                stats = zscore_stats[field]
                mean = float(stats["mean"])
                std = float(stats.get("std", 1))
                if std == 0:
                    continue
                z = (value - mean) / std
                status = self._classify_zscore(z)
                impact = ImpactLevel.HIGH if status == DQStatus.INVALID else ImpactLevel.MEDIUM
                results.append(self._make_result(
                    record=record,
                    field_name=field,
                    field_value=str(value),
                    status=status,
                    impact=impact,
                    rule_applied=(
                        f"Z-Score {field}: Z={z:.2f} "
                        f"(V<=2.0, S<3.5, IV>=3.5)"
                    ),
                    details={
                        "algorithm": "zscore",
                        "value": value,
                        "mean": mean,
                        "std": std,
                        "z_score": round(z, 4),
                        "abs_z": round(abs(z), 4),
                    },
                ))
                continue  # Z-score est le plus robuste — pas besoin d'autres algos

            # ── Algorithme 2 — Comparison to Average ──────────────────────
            if field in historical_means:
                hist_mean = historical_means[field]
                if hist_mean == 0:
                    continue
                pct = abs(value - hist_mean) / hist_mean * 100
                status = self._classify_pct_deviation(pct, _CTA_V, _CTA_IV)
                impact = ImpactLevel.HIGH if status == DQStatus.INVALID else ImpactLevel.MEDIUM
                results.append(self._make_result(
                    record=record,
                    field_name=field,
                    field_value=str(value),
                    status=status,
                    impact=impact,
                    rule_applied=(
                        f"Comparison to Average '{field}': écart {pct:.1f}% "
                        f"(V<{_CTA_V}%, S<{_CTA_IV}%, IV>={_CTA_IV}%)"
                    ),
                    details={
                        "algorithm": "comparison_to_average",
                        "value": value,
                        "historical_mean": hist_mean,
                        "pct_deviation": round(pct, 2),
                        "v_threshold_pct": _CTA_V,
                        "iv_threshold_pct": _CTA_IV,
                    },
                ))
                continue

            # ── Algorithme 1 — Prior Value Comparison ─────────────────────
            if field in prior_values:
                prior = prior_values[field]
                avg = (value + prior) / 2
                if avg == 0:
                    continue
                pct = abs(value - prior) / avg * 100
                status = self._classify_pct_deviation(pct, _PVC_V, _PVC_IV)
                impact = ImpactLevel.MEDIUM if status != DQStatus.VALID else ImpactLevel.LOW
                results.append(self._make_result(
                    record=record,
                    field_name=field,
                    field_value=str(value),
                    status=status,
                    impact=impact,
                    rule_applied=(
                        f"Prior Value Comparison '{field}': évolution {pct:.1f}% "
                        f"(V<{_PVC_V}%, S<{_PVC_IV}%, IV>={_PVC_IV}%)"
                    ),
                    details={
                        "algorithm": "prior_value_comparison",
                        "value": value,
                        "prior_value": prior,
                        "pct_change": round(pct, 2),
                        "v_threshold_pct": _PVC_V,
                        "iv_threshold_pct": _PVC_IV,
                    },
                ))

        return results
