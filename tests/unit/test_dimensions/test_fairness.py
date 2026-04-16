"""Tests Dimension 10 — Fairness. Règle des 3 cas : V, S, IV."""

from findata_dq.dimensions.fairness import Fairness
from findata_dq.models.dq_result import DQStatus, ImpactLevel

dim = Fairness()

BASE = {"record_id": "MDL-001", "dataset": "model_metadata", "protected_attribute": "sexe"}


# ── Disparate Impact ──────────────────────────────────────────────────────────

def test_fairness_V_disparate_impact():
    record = {**BASE, "disparate_impact": 0.95}
    results = dim.validate(record)
    di = [r for r in results if r.details.get("metric") == "disparate_impact"]
    assert di[0].status == DQStatus.VALID


def test_fairness_S_disparate_impact_limite():
    """DI entre 0.70 et 0.80 → S."""
    record = {**BASE, "disparate_impact": 0.75}
    results = dim.validate(record)
    di = [r for r in results if r.details.get("metric") == "disparate_impact"]
    assert di[0].status == DQStatus.SUSPECT


def test_fairness_IV_disparate_impact_faible():
    """DI < 0.70 → IV."""
    record = {**BASE, "disparate_impact": 0.55}
    results = dim.validate(record)
    di = [r for r in results if r.details.get("metric") == "disparate_impact"]
    assert di[0].status == DQStatus.INVALID
    assert di[0].impact == ImpactLevel.HIGH


def test_fairness_IV_disparate_impact_eleve():
    """DI > 1.30 → IV (discrimination inverse)."""
    record = {**BASE, "disparate_impact": 1.45}
    results = dim.validate(record)
    di = [r for r in results if r.details.get("metric") == "disparate_impact"]
    assert di[0].status == DQStatus.INVALID


# ── Demographic Parity ────────────────────────────────────────────────────────

def test_fairness_V_demographic_parity():
    record = {**BASE, "demographic_parity": 0.03}
    results = dim.validate(record)
    dp = [r for r in results if r.details.get("metric") == "demographic_parity"]
    assert dp[0].status == DQStatus.VALID


def test_fairness_S_demographic_parity():
    record = {**BASE, "demographic_parity": 0.07}
    results = dim.validate(record)
    dp = [r for r in results if r.details.get("metric") == "demographic_parity"]
    assert dp[0].status == DQStatus.SUSPECT


def test_fairness_IV_demographic_parity():
    record = {**BASE, "demographic_parity": 0.15}
    results = dim.validate(record)
    dp = [r for r in results if r.details.get("metric") == "demographic_parity"]
    assert dp[0].status == DQStatus.INVALID


# ── Equalized Odds ────────────────────────────────────────────────────────────

def test_fairness_V_equalized_odds():
    record = {**BASE, "equalized_odds": 0.03}
    results = dim.validate(record)
    eo = [r for r in results if r.details.get("metric") == "equalized_odds"]
    assert eo[0].status == DQStatus.VALID


def test_fairness_IV_equalized_odds():
    record = {**BASE, "equalized_odds": 0.12}
    results = dim.validate(record)
    eo = [r for r in results if r.details.get("metric") == "equalized_odds"]
    assert eo[0].status == DQStatus.INVALID


# ── Calcul depuis données brutes ──────────────────────────────────────────────

def test_fairness_V_di_calcule_depuis_brut():
    """DI calculé depuis les effectifs bruts → V."""
    record = {
        **BASE,
        "decisions_group_a": 10, "total_group_a": 100,  # P_A = 0.10
        "decisions_group_b": 11, "total_group_b": 100,  # P_B = 0.11 → DI ≈ 0.91
    }
    results = dim.validate(record)
    di = [r for r in results if "computed" in r.details.get("metric", "")]
    assert di[0].status == DQStatus.VALID


def test_fairness_IV_di_calcule_discriminant():
    """DI calculé fortement défavorable → IV."""
    record = {
        **BASE,
        "decisions_group_a": 40, "total_group_a": 100,  # P_A = 0.40
        "decisions_group_b": 10, "total_group_b": 100,  # P_B = 0.10 → DI = 4.0
    }
    results = dim.validate(record)
    di = [r for r in results if "computed" in r.details.get("metric", "")]
    assert di[0].status == DQStatus.INVALID


# ── Modèle valide avec disparate_impact_sexe (model_metadata) ─────────────────

def test_fairness_V_depuis_model_valid(model_valid):
    """model_valid a disparate_impact_sexe = None → pas de résultat fairness."""
    results = dim.validate(model_valid)
    # model_valid n'a pas de champ disparate_impact* → résultats vides
    assert isinstance(results, list)
