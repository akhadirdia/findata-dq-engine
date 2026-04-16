"""
Dimension 10 — Fairness (Équité & Biais IA)

Logique : Détecter les discriminations dans les décisions algorithmiques
selon les attributs protégés (tarification, scoring risque).

Métriques (CLAUDE.md section 4) :

1. Disparate Impact Ratio :
   DI = P(défavorable|groupe_A) / P(défavorable|groupe_B)
   V  : DI dans [0.80, 1.25]   (seuil légal 4/5ths rule + AI Act)
   S  : DI dans [0.70, 0.80] ou [1.25, 1.30]
   IV : DI < 0.70 ou DI > 1.30, Impact H

2. Demographic Parity :
   DP = |P(score_haut|A) - P(score_haut|B)|
   V  : DP < 0.05
   S  : 0.05 <= DP < 0.10
   IV : DP >= 0.10, Impact H

3. Equalized Odds :
   EOdds = |TPR_groupe_A - TPR_groupe_B|
   V  : EOdds < 0.05
   IV : EOdds >= 0.10, Impact H
"""

from __future__ import annotations

from typing import Any

from findata_dq.dimensions.base import BaseDimension
from findata_dq.models.dq_result import DQResult, DQStatus, ImpactLevel

# Seuils Disparate Impact (4/5ths rule + AI Act)
_DI_V_LOW, _DI_V_HIGH = 0.80, 1.25
_DI_S_LOW, _DI_S_HIGH = 0.70, 1.30

# Seuils Demographic Parity
_DP_V = 0.05
_DP_IV = 0.10

# Seuils Equalized Odds
_EO_V = 0.05
_EO_IV = 0.10


def _classify_di(di: float) -> str:
    if _DI_V_LOW <= di <= _DI_V_HIGH:
        return DQStatus.VALID
    if _DI_S_LOW <= di <= _DI_S_HIGH:
        return DQStatus.SUSPECT
    return DQStatus.INVALID


def _classify_gap(gap: float, v_threshold: float, iv_threshold: float) -> str:
    if gap < v_threshold:
        return DQStatus.VALID
    if gap < iv_threshold:
        return DQStatus.SUSPECT
    return DQStatus.INVALID


class Fairness(BaseDimension):
    """
    Dimension 10 — Fairness.
    Détecte les biais algorithmiques sur les attributs protégés.
    Référence : AI Act Art. 10(3), Loi 25 Québec, RGPD Art. 22.
    """

    name = "Fairness"
    description = "Mesure les biais algorithmiques (DI, Demographic Parity, Equalized Odds) sur attributs protégés."
    default_impact = ImpactLevel.HIGH

    def validate(
        self,
        record: dict[str, Any],
        config: dict[str, Any] = {},
    ) -> list[DQResult]:
        """
        Ce validator opère sur un enregistrement de type FairnessMetrics
        (déjà calculé en amont par le pipeline), pas sur une ligne de données brutes.

        Paramètres config :
          dataset : str   — 'model_metadata' | 'fairness_metrics'

        Champs attendus dans record :
          disparate_impact      : float  — DI calculé
          demographic_parity    : float  — DP calculé
          equalized_odds        : float  — EO calculé
          protected_attribute   : str    — attribut testé (ex: 'sexe')
          model_id              : str

        Mode agrégé (calcul depuis des données brutes) :
          decisions_group_a     : int    — nb décisions défavorables groupe A
          total_group_a         : int    — effectif groupe A
          decisions_group_b     : int
          total_group_b         : int
          high_scores_group_a   : int    — nb scores élevés groupe A
          high_scores_group_b   : int
        """
        results: list[DQResult] = []
        protected_attr = record.get("protected_attribute", "attribut_protégé")

        # ── Mode 1 : métriques pré-calculées ─────────────────────────────
        di = record.get("disparate_impact") or record.get("disparate_impact_sexe")
        dp = record.get("demographic_parity")
        eo = record.get("equalized_odds")

        if di is not None:
            try:
                di_val = float(di)
                status = _classify_di(di_val)
                impact = ImpactLevel.HIGH if status == DQStatus.INVALID else ImpactLevel.MEDIUM
                results.append(self._make_result(
                    record=record,
                    field_name=f"disparate_impact_{protected_attr}",
                    field_value=str(round(di_val, 4)),
                    status=status,
                    impact=impact,
                    rule_applied=(
                        f"Disparate Impact ({protected_attr}): DI={di_val:.4f} "
                        f"(V:[{_DI_V_LOW},{_DI_V_HIGH}], S:[{_DI_S_LOW},{_DI_S_HIGH}], "
                        f"IV hors [{_DI_S_LOW},{_DI_S_HIGH}])."
                    ),
                    details={
                        "metric": "disparate_impact",
                        "protected_attribute": protected_attr,
                        "value": di_val,
                        "v_range": [_DI_V_LOW, _DI_V_HIGH],
                        "s_range": [_DI_S_LOW, _DI_S_HIGH],
                        "regulation": "AI Act Art.10(3) / 4/5ths rule",
                    },
                ))
            except (ValueError, TypeError):
                pass

        if dp is not None:
            try:
                dp_val = abs(float(dp))
                status = _classify_gap(dp_val, _DP_V, _DP_IV)
                impact = ImpactLevel.HIGH if status == DQStatus.INVALID else ImpactLevel.MEDIUM
                results.append(self._make_result(
                    record=record,
                    field_name=f"demographic_parity_{protected_attr}",
                    field_value=str(round(dp_val, 4)),
                    status=status,
                    impact=impact,
                    rule_applied=(
                        f"Demographic Parity ({protected_attr}): |DP|={dp_val:.4f} "
                        f"(V<{_DP_V}, S<{_DP_IV}, IV>={_DP_IV})."
                    ),
                    details={
                        "metric": "demographic_parity",
                        "protected_attribute": protected_attr,
                        "value": dp_val,
                    },
                ))
            except (ValueError, TypeError):
                pass

        if eo is not None:
            try:
                eo_val = abs(float(eo))
                status = _classify_gap(eo_val, _EO_V, _EO_IV)
                impact = ImpactLevel.HIGH if status == DQStatus.INVALID else ImpactLevel.MEDIUM
                results.append(self._make_result(
                    record=record,
                    field_name=f"equalized_odds_{protected_attr}",
                    field_value=str(round(eo_val, 4)),
                    status=status,
                    impact=impact,
                    rule_applied=(
                        f"Equalized Odds ({protected_attr}): |EOdds|={eo_val:.4f} "
                        f"(V<{_EO_V}, IV>={_EO_IV})."
                    ),
                    details={
                        "metric": "equalized_odds",
                        "protected_attribute": protected_attr,
                        "value": eo_val,
                    },
                ))
            except (ValueError, TypeError):
                pass

        # ── Mode 2 : calcul DI depuis données brutes ──────────────────────
        if di is None and all(
            k in record for k in ("decisions_group_a", "total_group_a", "decisions_group_b", "total_group_b")
        ):
            try:
                p_a = record["decisions_group_a"] / record["total_group_a"]
                p_b = record["decisions_group_b"] / record["total_group_b"]
                if p_b > 0:
                    di_computed = p_a / p_b
                    status = _classify_di(di_computed)
                    impact = ImpactLevel.HIGH if status == DQStatus.INVALID else ImpactLevel.MEDIUM
                    results.append(self._make_result(
                        record=record,
                        field_name=f"disparate_impact_{protected_attr}",
                        field_value=str(round(di_computed, 4)),
                        status=status,
                        impact=impact,
                        rule_applied=(
                            f"Disparate Impact calculé ({protected_attr}): DI={di_computed:.4f} "
                            f"(P_A={p_a:.3f}, P_B={p_b:.3f})."
                        ),
                        details={
                            "metric": "disparate_impact_computed",
                            "p_a": round(p_a, 4),
                            "p_b": round(p_b, 4),
                            "di": round(di_computed, 4),
                        },
                    ))
            except (KeyError, ZeroDivisionError, TypeError):
                pass

        return results
