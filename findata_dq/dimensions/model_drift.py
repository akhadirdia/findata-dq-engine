"""
Dimension 11 — ModelDrift (Dérive des Modèles IA)

Logique : Détecter quand un modèle en production se dégrade parce que
les données de production divergent des données d'entraînement.

1. PSI — Population Stability Index (feature drift) :
   PSI = Σ (actual_pct - expected_pct) × ln(actual_pct / expected_pct)
   V  : PSI < 0.10
   S  : 0.10 <= PSI < 0.25
   IV : PSI >= 0.25  → retraining requis

2. Performance Drift (dégradation métrique) :
   delta = accuracy_production - accuracy_baseline
   V  : |delta| < 0.02
   S  : 0.02 <= |delta| < 0.05
   IV : |delta| >= 0.05

3. Prediction Distribution Drift (KL divergence) :
   KL = Σ P(x) × log(P(x) / Q(x))
   V  : KL < 0.05
   IV : KL >= 0.20
"""

from __future__ import annotations

import math
import os
from typing import Any

from findata_dq.dimensions.base import BaseDimension
from findata_dq.models.dq_result import DQResult, DQStatus, ImpactLevel

# Seuils PSI (CLAUDE.md section 4)
_PSI_V  = float(os.getenv("PSI_S_THRESHOLD", "0.10"))
_PSI_IV = float(os.getenv("PSI_IV_THRESHOLD", "0.25"))

# Seuils Performance Drift
_PERF_V  = 0.02
_PERF_S  = 0.05

# Seuils KL divergence
_KL_V  = 0.05
_KL_IV = 0.20


def _classify_psi(psi: float) -> str:
    if psi < _PSI_V:
        return DQStatus.VALID
    if psi < _PSI_IV:
        return DQStatus.SUSPECT
    return DQStatus.INVALID


def _classify_perf_delta(delta: float) -> str:
    abs_delta = abs(delta)
    if abs_delta < _PERF_V:
        return DQStatus.VALID
    if abs_delta < _PERF_S:
        return DQStatus.SUSPECT
    return DQStatus.INVALID


def _classify_kl(kl: float) -> str:
    if kl < _KL_V:
        return DQStatus.VALID
    if kl < _KL_IV:
        return DQStatus.SUSPECT
    return DQStatus.INVALID


def _compute_psi(actual: list[float], expected: list[float]) -> float:
    """
    Calcule le PSI entre deux distributions de probabilités.
    Les listes doivent être de même longueur et sommer à 1.
    Petit epsilon pour éviter log(0).
    """
    eps = 1e-6
    psi = 0.0
    for a, e in zip(actual, expected, strict=False):
        a = max(a, eps)
        e = max(e, eps)
        psi += (a - e) * math.log(a / e)
    return psi


def _compute_kl(p: list[float], q: list[float]) -> float:
    """Calcule la divergence KL entre deux distributions."""
    eps = 1e-6
    kl = 0.0
    for pi, qi in zip(p, q, strict=False):
        pi = max(pi, eps)
        qi = max(qi, eps)
        kl += pi * math.log(pi / qi)
    return kl


class ModelDrift(BaseDimension):
    """
    Dimension 11 — ModelDrift.
    Détecte la dérive des modèles IA en production (PSI, perf delta, KL divergence).
    """

    name = "ModelDrift"
    description = "Détecte la dérive des modèles IA : PSI feature drift, performance delta, KL divergence."
    default_impact = ImpactLevel.HIGH

    def validate(
        self,
        record: dict[str, Any],
        config: dict[str, Any] | None = None,
    ) -> list[DQResult]:
        config = config or {}
        """
        Paramètres config :
          accuracy_baseline : float          — accuracy de référence (entraînement)
          psi_distributions : dict           — {feature: {"actual": [...], "expected": [...]}}
          kl_distributions  : dict           — {"actual": [...], "expected": [...]}

        Champs attendus dans record (model_metadata) :
          drift_score   : float   — PSI global pré-calculé
          drift_status  : str     — V|S|IV pré-calculé
          accuracy      : float   — accuracy en production
        """
        results: list[DQResult] = []

        # ── 1. PSI pré-calculé (depuis model_metadata) ───────────────────
        drift_score = record.get("drift_score")
        if drift_score is not None:
            try:
                psi = float(drift_score)
                status = _classify_psi(psi)
                impact = ImpactLevel.HIGH if status == DQStatus.INVALID else ImpactLevel.MEDIUM
                results.append(self._make_result(
                    record=record,
                    field_name="drift_score",
                    field_value=str(round(psi, 4)),
                    status=status,
                    impact=impact,
                    rule_applied=(
                        f"PSI global : {psi:.4f} "
                        f"(V<{_PSI_V}, S<{_PSI_IV}, IV>={_PSI_IV}). "
                        + ("Retraining requis." if status == DQStatus.INVALID else "Stable.")
                    ),
                    details={
                        "algorithm": "PSI",
                        "psi_value": psi,
                        "v_threshold": _PSI_V,
                        "iv_threshold": _PSI_IV,
                    },
                ))
            except (ValueError, TypeError):
                pass

        # ── 2. PSI calculé depuis distributions brutes ────────────────────
        for feature, distrib in config.get("psi_distributions", {}).items():
            actual = distrib.get("actual", [])
            expected = distrib.get("expected", [])
            if actual and expected and len(actual) == len(expected):
                psi = _compute_psi(actual, expected)
                status = _classify_psi(psi)
                impact = ImpactLevel.HIGH if status == DQStatus.INVALID else ImpactLevel.MEDIUM
                results.append(self._make_result(
                    record=record,
                    field_name=f"psi_{feature}",
                    field_value=str(round(psi, 4)),
                    status=status,
                    impact=impact,
                    rule_applied=f"PSI feature '{feature}': {psi:.4f}.",
                    details={"algorithm": "PSI", "feature": feature, "psi_value": round(psi, 4)},
                ))

        # ── 3. Performance Drift ──────────────────────────────────────────
        accuracy_prod = record.get("accuracy")
        accuracy_baseline = config.get("accuracy_baseline")
        if accuracy_prod is not None and accuracy_baseline is not None:
            try:
                delta = float(accuracy_prod) - float(accuracy_baseline)
                status = _classify_perf_delta(delta)
                impact = ImpactLevel.HIGH if status == DQStatus.INVALID else ImpactLevel.MEDIUM
                results.append(self._make_result(
                    record=record,
                    field_name="accuracy",
                    field_value=str(round(float(accuracy_prod), 4)),
                    status=status,
                    impact=impact,
                    rule_applied=(
                        f"Performance Drift : delta_accuracy={delta:+.4f} "
                        f"(V: |delta|<{_PERF_V}, S<{_PERF_S}, IV>={_PERF_S})."
                    ),
                    details={
                        "algorithm": "performance_drift",
                        "accuracy_prod": float(accuracy_prod),
                        "accuracy_baseline": float(accuracy_baseline),
                        "delta": round(delta, 4),
                        "abs_delta": round(abs(delta), 4),
                    },
                ))
            except (ValueError, TypeError):
                pass

        # ── 4. KL Divergence ─────────────────────────────────────────────
        kl_distrib = config.get("kl_distributions")
        if kl_distrib:
            p = kl_distrib.get("actual", [])
            q = kl_distrib.get("expected", [])
            if p and q and len(p) == len(q):
                kl = _compute_kl(p, q)
                status = _classify_kl(kl)
                impact = ImpactLevel.HIGH if status == DQStatus.INVALID else ImpactLevel.MEDIUM
                results.append(self._make_result(
                    record=record,
                    field_name="prediction_distribution",
                    field_value=str(round(kl, 4)),
                    status=status,
                    impact=impact,
                    rule_applied=(
                        f"KL Divergence prédictions : {kl:.4f} "
                        f"(V<{_KL_V}, S<{_KL_IV}, IV>={_KL_IV})."
                    ),
                    details={"algorithm": "KL_divergence", "kl_value": round(kl, 4)},
                ))

        return results
